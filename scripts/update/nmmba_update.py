import numpy as np
from numpy import nan
import requests
import pandas as pd
import bson
import time
import os
from datetime import datetime, timedelta

import math 

from dotenv import load_dotenv
import os
load_dotenv(override=True)

from scripts.taxon.match_utils import matching_flow_new_optimized, match_cols
from scripts.utils import *

records_processor = OptimizedRecordsProcessor(db, batch_size=200)
matchlog_processor = OptimizedMatchLogProcessor(db, batch_size=300)

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName','sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 


# 單位資訊
group = 'nmmba'
rights_holder = '國立海洋生物博物館生物典藏管理系統'

# 在portal.Partner.info裡面的id
info_id = 0


response = requests.get(f'http://solr:8983/solr/tbia_records/select?fl=update_version&fq=rightsHolder:"{rights_holder}"&q.op=OR&q=*%3A*&rows=1&sort=update_version%20desc')
if response.status_code == 200:
    resp = response.json()
    if data := resp['response']['docs']:
        update_version = data[0]['update_version'] + 1
    else:
        update_version = 1

# 在開始之前 先確認存不存在 
# 若不存在 insert一個新的update_version
current_page, note = insert_new_update_version(rights_holder=rights_holder,update_version=update_version)


token = os.getenv('NMMBA_TOKEN')
headers = {
    "Authorization": f"Bearer {token}"
}

now = datetime.now() + timedelta(hours=8) 

c = current_page if current_page > 0 else 1
has_more_data = True
should_stop = False





# 1. [核心邏輯] 判斷並拆分逗號混合的座標
def resolve_mixed_comma(row):
    """
    輸入一行資料，檢查 longitude_start 是否含有逗號。
    若有，則拆分並判斷經緯度歸位；若無，則回傳原值。
    回傳順序固定為: lon_start, lon_end, lat_start, lat_end
    """
    l_start = str(row['longitude_start']).strip()
    # 如果沒有逗號，直接回傳原本的四個欄位，不做更動
    if ',' not in l_start:
        return row['longitude_start'], row['longitude_end'], row['latitude_start'], row['latitude_end']
    # 處理含有逗號的情況
    parts = [p.strip() for p in l_start.split(',')]
    # 預設回傳值 (萬一解析失敗則回傳原值)
    if len(parts) != 2:
        return row['longitude_start'], row['longitude_end'], row['latitude_start'], row['latitude_end']
    p1, p2 = parts[0], parts[1]
    # --- 判斷 p1 是緯度還是經度 ---
    p1_is_lat = False
    p1_u = p1.upper()
    # A. 先看方向標示
    if 'N' in p1_u or 'S' in p1_u:
        p1_is_lat = True
    elif 'E' in p1_u or 'W' in p1_u:
        p1_is_lat = False
    else:
        # B. 若無方向，看數值 (假設 <= 90 為緯度)
        nums = re.findall(r"(\d+(?:\.\d*)?|\.\d+)", p1)
        if nums and float(nums[0]) <= 90:
            p1_is_lat = True
    # --- 歸位並清空 End 欄位 ---
    if p1_is_lat:
        return p2, np.nan, p1, np.nan  # lon=p2, lat=p1
    else:
        return p1, np.nan, p2, np.nan  # lon=p1, lat=p2


# 2. [核心邏輯] 合併 Start/End (您原本的函式，稍微縮減寫法)
def format_merge_text(start, end):
    """
    合併 Start 與 End 欄位。
    若 Start == End 或 End 為空，只回傳 Start。
    否則回傳 Start-End。
    """
    s = str(start).strip() if pd.notna(start) and str(start).lower() != 'nan' else ''
    e = str(end).strip() if pd.notna(end) and str(end).lower() != 'nan' else ''
    if not s:
        return ''
    if not e or s == e:
        return s
    return f"{s}-{e}"


# 3. [主流程] 封裝成一個清理函式
def process_coordinate_cleaning(df):
    df_clean = df.copy()
    # 步驟一：處理逗號錯置 (一次更新四個欄位)
    # 使用 apply 搭配 result_type='expand' 可以直接展開成四個欄位
    cols_to_fix = ['longitude_start', 'longitude_end', 'latitude_start', 'latitude_end']
    df_clean[cols_to_fix] = df_clean.apply(resolve_mixed_comma, axis=1, result_type='expand')
    # 步驟二：處理 Start/End 合併
    df_clean['verbatimLongitude'] = df_clean.apply(
        lambda row: format_merge_text(row['longitude_start'], row['longitude_end']), axis=1
    )
    df_clean['verbatimLatitude'] = df_clean.apply(
        lambda row: format_merge_text(row['latitude_start'], row['latitude_end']), axis=1
    )
    return df_clean



# while has_more_data:
#     # data = []
#     # time.sleep(1)
#     url = f"https://helloocean.nmmba.gov.tw/nmmba_front/API/get.aspx?pageIndex={c}"
#     response = requests.post(url, headers=headers)
#     if response.status_code == 200:
#         result = response.json()
#         total_page = result['totalPages']
#         data += result.get('data')
#         print('page:',c , 'total page:', total_page)
#         if c >= total_page:
#             has_more_data = False
#             break
#         c+=1
#     else:
#         print(f"Error: HTTP {response.status_code}")
#         should_stop = True
#         break  # 跳出內層 while
#     if should_stop:
#         break # 跳出外層 while

# >>> df.mode_ch.unique()
# array(['一般標本', '副模標本', '珍稀標本', '一般展示品', '保育類展示品(大型 >50cm)', '保育類展示品', '',
#        '一般展示品(大型 >50cm)', '正模標本', '新模標本', '副選模標', '選模標本', '一般教具'],
#       dtype=object)

# >>> df.status_ch.unique()
# array(['已上架', '全部借出', '待入庫', '展示中', '未尋獲', '其它', '註銷', '部分借出', '贈送', '交換',
#        '遺失'], dtype=object)

# >>> df.country_ch.unique()
# array(['台灣', '美國', '日本', '巴西', '越南', '', '印尼', '中國大陸', '羅馬尼亞', '澳大利亞',
#        '泰國', '印度', '香港', '菲律賓', '阿拉伯聯合大公國', '馬來西亞', '埃及', '新加坡', '萬那度',
#        '東非', '吉里巴斯', '紐西蘭', '新喀里多尼亞', '斐濟', '馬紹爾群島', '坦尚尼亞', '馬爾地夫',
#        '南極洲', '寮國', '墨西哥', '肯亞', '南非', '蓋亞那', '蘇利南', '巴拿馬', '委內瑞拉',
#        '哥倫比亞', '厄瓜多爾', '秘魯', '多明尼加', '俄羅斯', '緬甸', '義大利', '未知', '千里達及托貝哥',
#        '沙烏地阿拉伯', '孟加拉', '韓國', '巴哈馬', '智利', '聖文森及格瑞那丁', '加拿大', '馬達加斯加'],
#       dtype=object)


while has_more_data:
    data = []
    p = c + 10
    while c < p and has_more_data:
        # offset = 300 * c
        time.sleep(1)
        url = f"https://helloocean.nmmba.gov.tw/nmmba_front/API/get.aspx?pageIndex={c}"
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            result = response.json()
            total_page = result['totalPages']
            now_mediaLicense = result['mediaLicense']
            now_license = result['license']
            data += result.get('data')
            print('page:',c , 'total page:', total_page)
            if c >= total_page:
                has_more_data = False
                break
            c+=1
        else:
            print(f"Error: HTTP {response.status_code}")
            should_stop = True
            break  # 跳出內層 while
    if should_stop:
        break # 跳出外層 while
    if len(data):
        df = pd.DataFrame(data)
        df = df.replace(to_quote_dict)
        # 如果學名相關的欄位都是空值才排除
        df['license'] = now_license
        df['mediaLicense'] = now_mediaLicense
        df['sourceScientificName'] = df.apply(lambda x: x.genus_en + ' ' + x.species_en, axis=1)
        df = df.rename(columns={'nmmba_id': 'catalogNumber', # 館藏號
                                'id': 'reference_id', # 館藏號
                                'quantity': 'organismQuantity',
                                'collect_date': 'eventDate',
                                'family_en': 'sourceFamily',
                                'species_ch': 'sourceVernacularName', 
                                'mode_ch': 'typeStatus',
                                'created_at': 'sourceCreated',
                                'modified_at': 'sourceModified',
                                'preserve_ch': 'preservation',
                                'quantity': 'organismQuantity',
                                'associatedMedias': 'associatedMedia',
                                }
                        ) 
        df = df[~((df.sourceScientificName=='')&(df.sourceVernacularName==''))]
        if 'sensitiveCategory' in df.keys():
            df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]
        if 'license' in df.keys():
            df = df[(df.license!='無法辨識授權')&(df.license!='')&(~df.license.str.contains('ND|nd',regex=True))]
        else:
            df = []
        media_rule_list = []
        if len(df):
            df = df.reset_index(drop=True)
            df = df.replace(to_quote_dict)
            # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
            df['id'] = df.apply(lambda x: str(bson.objectid.ObjectId()), axis=1)
            df['locality'] = df.apply(lambda x: x.location_ch + ' ' + x.location_en, axis=1)
            df['recordedBy'] = df.apply(lambda x: x.collector_ch + ' ' + x.collector_en, axis=1)
            # 經緯度預處理
            df = process_coordinate_cleaning(df)
            df['references'] = df.apply(lambda x: f"https://helloocean.nmmba.gov.tw/nmmba_front/SpecimenDetail.aspx?id={x.reference_id}", axis=1)
            for col in cols_str_ends:
                if col in df.keys():
                    df[col] = df[col].apply(check_id_str_ends)
            sci_names = df[sci_cols].drop_duplicates().reset_index(drop=True)
            sci_names['sci_index'] = sci_names.index
            df = df.merge(sci_names)
            match_results = matching_flow_new_optimized(sci_names)
            df = df.drop(columns=['taxonID'], errors='ignore')
            if len(match_results):
                df = df.merge(match_results[match_cols], on='sci_index', how='left')
            df['sourceCreated'] = df['sourceCreated'].apply(lambda x: convert_date(x))
            df['sourceModified'] = df['sourceModified'].apply(lambda x: convert_date(x))
            df['group'] = group
            df['rightsHolder'] = rights_holder
            df['created'] = now
            df['modified'] = now
            df['recordType'] = 'col'
            # 出現地
            if 'locality' in df.keys():
                df['locality'] = df['locality'].apply(lambda x: x.strip() if x else x)
            # 數量 
            df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
            # basisOfRecord 無資料
            # 敏感層級 無資料
            #  如果有mediaLicense才放associatedMedia
            if 'mediaLicense' in df.keys() and 'associatedMedia' in df.keys():
                df['associatedMedia'] = df['associatedMedia'].apply(lambda x: (';').join(['https://helloocean.nmmba.gov.tw/nmmba_front/SpecimenPicture/' + xx.get('name') for xx in x]))
                df['associatedMedia'] = df['associatedMedia'].replace({None: '', np.nan: ''})
                df['associatedMedia'] = df.apply(lambda x: x.associatedMedia if x.mediaLicense else '', axis=1)
                df['media_rule_list'] = df[df.associatedMedia.notnull()]['associatedMedia'].apply(lambda x: get_media_rule(x))
                media_rule_list += list(df[df.media_rule_list.notnull()].media_rule_list.unique())
            # 地理資訊
            for g in geo_wo_raw_keys:
                if g not in df.keys():
                    df[g] = ''
            df[geo_wo_raw_keys] = df.apply(lambda x: pd.Series(create_grid_data_new(x.verbatimLongitude, x.verbatimLatitude)),  axis=1)
            # 年月日
            df[date_keys] = df.apply(lambda x: pd.Series(convert_year_month_day_new(x.to_dict())), axis=1)
            for d_col in ['year','month','day']:
                if d_col in df.keys():
                    df[d_col] = df[d_col].fillna(0).astype(int).replace({0: None})
            df = df.replace(to_quote_dict)
            df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
            # 資料集
            df['datasetName'] = '國立海洋生物博物館生物典藏管理系統' # 幫忙補
            ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
            # return tbiaDatasetID 並加上去
            return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version, group=group)
            df = df.merge(return_dataset_id)
            # 取得已建立的tbiaID
            df['catalogNumber'] = df['catalogNumber'].astype('str')
            if 'occurrenceID' not in df.keys():
                df['occurrenceID'] = ''
            else:
                df['occurrenceID'] = df['occurrenceID'].astype('str')
            existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])
            existed_records = get_existed_records_optimized(occ_ids=df[df.occurrenceID!='']['occurrenceID'].to_list(), rights_holder=rights_holder, cata_ids=df[df.catalogNumber!='']['catalogNumber'].to_list())
            existed_records = existed_records.replace({nan:''})
            # # 取得已建立的tbiaID
            # df['occurrenceID'] = df['occurrenceID'].astype('str')
            # if 'catalogNumber' not in df.keys():
            #     df['catalogNumber'] = ''
            # else:
            #     df['catalogNumber'] = df['catalogNumber'].astype('str')
            # existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])
            # existed_records = get_existed_records_optimized(occ_ids=df[df.occurrenceID!='']['occurrenceID'].to_list(), rights_holder=rights_holder, cata_ids=df[df.catalogNumber!='']['catalogNumber'].to_list())
            # existed_records = existed_records.replace({nan:''})
            if len(existed_records):
                df = df.merge(existed_records, how='left')
                df = df.replace(to_none_dict)
                df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
                df = df.drop(columns=['tbiaID'])
            df = df.replace(to_none_dict)
            # 更新match_log
            match_log = df[match_log_cols]
            match_log = match_log.reset_index(drop=True)
            match_log = create_match_log_df(match_log,now)
            matchlog_processor.smart_upsert_match_log(match_log, existed_records=existed_records)
            match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{c}.csv',index=None)
            # 用tbiaID更新records
            df['is_deleted'] = False
            df['update_version'] = int(update_version)
            df = df.rename(columns=({'id': 'tbiaID'}))
            df = df.drop(columns=[ck for ck in df.keys() if ck not in records_cols],errors='ignore')
            records_processor.smart_upsert_records(df, existed_records=existed_records)
            for mm in media_rule_list:
                update_media_rule(media_rule=mm,rights_holder=rights_holder)
    # 成功之後 更新update_update_version
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=None)


# 刪除is_deleted的records & match_log
delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))

# 打包match_log
zip_match_log(group=group,info_id=info_id)

# 更新update_version
update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)

# 更新 datahub - dataset
# update if deprecated
update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)


print('done!')

