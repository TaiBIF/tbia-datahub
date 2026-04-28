# 共通欄位建立/清理
import bson
import pandas as pd
from datetime import datetime
from dateutil import parser
import threading
import os
import psycopg2
from app import db_settings
import numpy as np

basis_dict = {
    "人為觀測": "HumanObservation",
    "機器觀測": "MachineObservation",
    "保存標本": "PreservedSpecimen",
    "材料樣本": "MaterialSample",
    "活體標本": "LivingSpecimen",
    "化石標本": "FossilSpecimen",
    "文獻紀錄": "MaterialCitation",
    "材料引用": "MaterialCitation", # GBIF資料
    "材料實體": "MaterialEntity",
    "組織樣本": "MaterialSample", # GBIF資料
    "分類群": "Taxon",
    "出現紀錄": "Occurrence",
    "調查活動": "Event",
    "人類調查": "HumanObservation", # GBIF資料,
    "Camera": "MachineObservation",
    "CameraTrap": "MachineObservation",
    "Event": "Event",
    "FossilSpecimen": "FossilSpecimen",
    "Human observation": "HumanObservation",
    "HumanObeservation": "HumanObservation",
    "HumanObservatio": "HumanObservation",
    "HumanObservation": "HumanObservation",
    "LivingSpecimen": "LivingSpecimen",
    "MachineObservation": "MachineObservation",
    "MachineObservation ": "MachineObservation",
    "MachineObservation (機器觀測)": "MachineObservation",
    "MaterialCitation": "MaterialCitation",
    "MaterialSample": "MaterialSample",
    "Occurrence": "Occurrence",
    "PreservedSpecimen": "PreservedSpecimen",
    "camera record": "MachineObservation",
    "event": "Event",
    "occurrence": "Occurrence",
    "出現記錄": "Occurrence",
    "機械觀測": "MachineObservation"
}



# 線程鎖，確保多個script同時運行時的檔案安全
_file_lock = threading.Lock()

def record_basis_of_record_values(df, csv_path='/code/basis_of_record_log.csv'):
    """
    記錄 basisOfRecord 欄位的所有原始值到 CSV 檔案中
    """
    if 'basisOfRecord' not in df.columns:
        return
    
    unique_values = df['basisOfRecord'].dropna().unique()
    if len(unique_values) == 0:
        return
    
    new_df = pd.DataFrame({'original_value': unique_values})
    
    with _file_lock:
        try:
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            new_df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False, encoding='utf-8')
        except Exception as e:
            print(f"記錄 basisOfRecord 值時發生錯誤: {e}")

def control_basis_of_record(basisOfRecord):
    if basisOfRecord in basis_dict.keys():
        basisOfRecord = basis_dict[basisOfRecord]
    else:
        basisOfRecord = ''
    return basisOfRecord


date_formats = [
    '%Y/%m/%d', '%Y%m%d', '%Y-%m-%d',
    '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M',
    '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %p %I:%M:%S', '%Y/%m/%d %H', '%Y-%m-%d %H',
    '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ', '%Y0%m0%d',
]


def convert_date(date):
    formatted_date = None
    if date == '' or date is None:
        return None

    date = str(date).replace('上午', 'AM').replace('下午', 'PM')

    for ff in date_formats:
        try:
            formatted_date = datetime.strptime(date, ff)
            break
        except:
            pass

    if not formatted_date:
        try:
            formatted_date = parser.parse(date)
        except:
            pass

    if not formatted_date:
        try:
            formatted_date = datetime.strptime(date.split('T')[0], '%Y-%m-%d')
        except:
            pass

    if not formatted_date:
        try:
            formatted_date = datetime.fromtimestamp(int(date))
        except:
            pass

    if formatted_date:
        # 解析出來的年份必須出現在原字串中
        if str(formatted_date.year) not in date:
            return None
        # 超過當下時間就拿掉
        if formatted_date > datetime.now(tz=formatted_date.tzinfo):
            return None

    return formatted_date

def convert_year_month_day(row):
    eventDate = row.get('eventDate')
    standardDate, year, month, day = None, None, None, None
    if standardDate := convert_date(eventDate):
        standardDate = standardDate.replace(tzinfo=None)
        year = standardDate.year
        month = standardDate.month
        day = standardDate.day
    elif row.get('year') and row.get('month') and row.get('day'):
        try:
            year = int(row.get('year'))
            month = int(row.get('month'))
            day = int(row.get('day'))
            if try_eventDate := convert_date('{}-{}-{}'.format(row.get('year'),row.get('month'),row.get('day'))):
                try_eventDate = try_eventDate.replace(tzinfo=None)
                year = try_eventDate.year
                month = try_eventDate.month
                day = try_eventDate.day
                eventDate = '{}-{}-{}'.format(year,month,day)
                standardDate = try_eventDate
        except:
            pass
    return [eventDate, standardDate, year, month, day]


def standardize_quantity(organismQuantity, individualCount=None):
    quantity = None

    try:
        if individualCount:
            quantity = int(individualCount)
        elif organismQuantity:
            quantity = float(organismQuantity)
    except:
        pass

    return quantity


cols_str_ends = ['catalogNumber', 'occurrenceID', 'recordNumber', 'scientificNameID', 'sourceTaxonID', 'sourceOccurrenceID']

# 用在pandas的apply
def check_id_str_ends(now_id):
    try:
        now_id = float(now_id)
        now_id = str(now_id)
        if now_id.endswith('.0'):
            now_id = now_id[:-2]
    except:
        now_id = str(now_id)
    return now_id


def apply_common_fields(df, group, rights_holder, now):
    """
    套用所有單位共通的欄位賦值與基本標準化。

    無條件:
        id, group, rightsHolder, created, modified, year/month/day
    欄位存在才做:
        sourceCreated, sourceModified, organismQuantity, locality

    不含 dataQuality (依賴後續處理，呼叫端最後自行呼叫):
        df['dataQuality'] = df.apply(calculate_data_quality, axis=1)
    """
    df['id'] = df.apply(lambda x: str(bson.objectid.ObjectId()), axis=1)
    df['group'] = group
    df['rightsHolder'] = rights_holder
    df['created'] = now
    df['modified'] = now

    for col in cols_str_ends:
        if col in df.keys():
            df[col] = df[col].apply(check_id_str_ends)

    if 'sourceCreated' in df.keys():
        df['sourceCreated'] = df['sourceCreated'].apply(convert_date)
    if 'sourceModified' in df.keys():
        df['sourceModified'] = df['sourceModified'].apply(convert_date)

    if 'organismQuantity' in df.keys():
        df['standardOrganismQuantity'] = df['organismQuantity'].apply(standardize_quantity)

    if 'locality' in df.keys():
        df['locality'] = df['locality'].apply(lambda x: x.strip() if x else x)

    if 'basisOfRecord' in df.keys():
        record_basis_of_record_values(df)
        df['basisOfRecord'] = df['basisOfRecord'].apply(control_basis_of_record)

    # 年月日
    date_keys = ['eventDate','standardDate','year','month','day']
    df[date_keys] = df.apply(lambda x: pd.Series(convert_year_month_day(x.to_dict())), axis=1)
    df[['year', 'month', 'day']] = df[['year', 'month', 'day']].fillna(0).astype(int).replace({0: None})

    return df


# 取得影像網址前綴
def _get_media_rule(media_url):
    full_rule = None
    string_list = media_url.split('//')
    if len(string_list) >= 2:
        protocol = string_list[0]
        domain = string_list[1].split('/')[0]
        full_rule = protocol + '//' + domain
    return full_rule

    # 應該會第一個 / 之前 
    # 例如:
    # https://inaturalist-open-data.s3.amazonaws.com/photos/244585305/large.jpg
    # https://brmas-media.s3.ap-northeast-1.amazonaws.com/hast/specimen/S_092293-l.jpg
    # https://fact.tfri.gov.tw/files/muse_fact/muse_styles/w960_m/mcode/ad080b9a9c3f6a5146f91efcf7c24481.jpg?itok=QwFOo8_E


def _extract_media_rules(media_str):
    """
    從 ';' 分隔的 URL 字串取出所有 media_rule (protocol+domain)。
    支援單一 URL 或多 URL；自動忽略空字串、保留出現順序、跨 URL 去重。
    """
    if not media_str:
        return []
    rules = []
    for url in media_str.split(';'):
        url = url.strip()
        if not url:
            continue
        rule = _get_media_rule(url)
        if rule and rule not in rules:
            rules.append(rule)
    return rules
 
 
def apply_media_rule(df, media_rule_list):
    """
    處理 mediaLicense + associatedMedia 區塊。
    若 mediaLicense 為空（或根本沒有該欄位）則清空 associatedMedia，並蒐集所有出現過的 media_rule。
    associatedMedia 支援 ';' 分隔多 URL，跨 domain 也能正確收集。
    """
    # 1. 如果根本沒有 associatedMedia 欄位，直接回傳即可
    if 'associatedMedia' not in df.keys():
            return df, []
    
    # 先把 associatedMedia 的 None 與 NaN 轉成空字串
    df['associatedMedia'] = df['associatedMedia'].replace({None: '', np.nan: ''})

    # 2. 處理 mediaLicense 條件
    if 'mediaLicense' not in df.keys():
        # 情況 A：如果連 mediaLicense 欄位都沒有，代表「全都沒有授權」，直接把 associatedMedia 全部清空
        df['associatedMedia'] = ''
    else:
        # 情況 B：有 mediaLicense 欄位，則依據該欄位的值是否為真 (Truthy) 來決定要不要保留 associatedMedia
        df['associatedMedia'] = df.apply(
            lambda x: x.associatedMedia if x.mediaLicense else '', axis=1
        )

    # 3. 展平所有 row 的 media_rule，跨 row 去重後再合進累積 list
    new_rules = set()
    for media_str in df['associatedMedia']:
        if media_str:
            new_rules.update(_extract_media_rules(media_str))
            
    for rule in new_rules:
        if rule not in media_rule_list:
            media_rule_list.append(rule)
            
    return df, list(new_rules)


def update_media_rules(media_rules, rights_holder, now):
    """媒體規則 UPSERT。吃 list of str，空 list 直接 return。"""
    if not media_rules:
        return
    conn = psycopg2.connect(**db_settings)
    try:
        with conn.cursor() as cur:
            query = """
                INSERT INTO media_rule ("rights_holder", "media_rule", "modified")
                VALUES (%s, %s, %s)
                ON CONFLICT ("rights_holder", "media_rule") DO UPDATE SET modified = %s
            """
            cur.executemany(
                query,
                [(rights_holder, r, now, now) for r in media_rules],
            )
        conn.commit()
    finally:
        conn.close()


def filter_by_taxon_fields(df, required_cols):
    """
    排除「指定欄位全部為空字串」的 row。
    (如果學名相關的欄位都是空值才排除)
    required_cols: 必須有值的欄位 list (任一欄位有值就保留)
    """
 
    # 缺失欄位視為空，組合「全空」mask
    mask_all_empty = pd.Series(True, index=df.index)
    for col in required_cols:
        if col in df.keys():
            mask_all_empty &= (df[col] == '')
        # 欄位不存在等同空，mask 維持 True
    return df[~mask_all_empty]
 
 
def filter_by_license_and_sensitivity(df):
    """
    過濾 sensitiveCategory 與 license。
    drop sensitive: 排除 sensitiveCategory in ['分類群不開放', '物種不開放']
    drop license:   排除無法辨識/空/含 ND|nd 的授權
 
    """
    if 'sensitiveCategory' in df.keys():
        df = df[~df['sensitiveCategory'].isin(['分類群不開放', '物種不開放'])]
 
    if 'license' in df.keys():
        df = df[
            (df['license'] != '無法辨識授權')
            & (df['license'] != '')
            & (~df['license'].str.contains('ND|nd', regex=True, na=False))
        ]
    return df


def apply_record_type(df, mode):
    """
    設定 recordType 欄位。
 
    mode:
        'occ'  - 固定 'occ'
        'col'  - 固定 'col'
        'auto' - 從 basisOfRecord 判斷 (含 specimen/標本 → col 否則 occ)
    """
    if mode == 'auto':
        df['recordType'] = np.where(
            df['basisOfRecord'].str.contains('specimen|標本', case=False, na=False),
            'col', 'occ'
        )
    elif mode in ('occ', 'col'):
        df['recordType'] = mode
    else:
        raise ValueError(f"Invalid mode: {mode!r}, must be 'occ' | 'col' | 'auto'")
 
    return df


# 1 銅: 只要沒有 scientificName、latitude、longitude、year （任何一項）
# 2 銀: 有 scientificName、latitude、longitude、year （這四個都有）
# 3 金: 有 scientificName、latitude、longitude、year、month、coordinatesUncertaintyInMeters / coordinatePrecision 其一 and basisOfRecord 


# 這邊sourceScientificName擴大到originalVernacularName
# basisOfRecord要是控制詞彙

# ['sourceScientificName','originalVernacularName','standardDate','year','month','standardLatitude','standardLongitude','standardRawLatitude','standardRawLongitude','coordinatesUncertaintyInMeters','coordinatePrecision','basisOfRecord']

def calculate_data_quality(row):
    # row = row.to_dict()
    if (row.get('sourceScientificName') or row.get('originalVernacularName')) and (row.get('standardDate') or (row.get('year') and row.get('month'))) and ((row.get('standardLatitude') and row.get('standardLongitude')) or (row.get('standardRawLatitude') and row.get('standardRawLongitude'))) and (row.get('coordinatesUncertaintyInMeters') or row.get('coordinatePrecision')) and ((row.get('basisOfRecord') in basis_dict.keys() )or (row.get('basisOfRecord') in basis_dict.values())):
        data_quality = 3
    elif (row.get('sourceScientificName') or row.get('originalVernacularName')) and (row.get('standardDate') or row.get('year'))  and ((row.get('standardLatitude') and row.get('standardLongitude')) or (row.get('standardRawLatitude') and row.get('standardRawLongitude'))):
        data_quality = 2
    else:
        data_quality = 1
    return data_quality
