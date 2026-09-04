# 共通欄位建立/清理
import bson
import pandas as pd
from datetime import datetime
from dateutil import parser
import threading
import os
import re
import psycopg2
from app import db_settings
import numpy as np
import requests
from numpy import nan
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from scripts.utils.progress import timed


to_none_dict = {nan: None, 
                'NA': None, 
                '-99999': None, 
                '-999999': None, 
                -99999: None, 
                -999999: None, 
                'N/A': None, 
                'nan': None, 
                '': None,
                'NaT': None,
                'NaN': None}

to_quote_dict = {nan: '', 
                 'NA': '', 
                 '-99999': '', 
                 '-999999': '', 
                 -99999: '', 
                 -999999: '', 
                 'N/A': '', 
                 'nan': '',
                 None: '',
                 'NaT': '',
                 'NaN': ''}


def get_gbif_id(gbifDatasetID, occurrenceID):
    gbif_url = f"https://api.gbif.org/v1/occurrence/{gbifDatasetID}/{occurrenceID}"
    gbif_resp = requests.get(gbif_url)
    gbifID = None
    if gbif_resp.status_code == 200:
        gbif_res = gbif_resp.json()
        gbifID = gbif_res.get('gbifID')
    return gbifID


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
            # 用兩個不同 default 解析，月/日若隨 default 變動代表原字串沒提供，
            # 視為無法解析成完整日期（避免 dateutil 用今天補齊年份字串）
            d1 = parser.parse(date, default=datetime(1900, 1, 1))
            d2 = parser.parse(date, default=datetime(1901, 2, 2))
            if d1.month == d2.month and d1.day == d2.day:
                formatted_date = d1
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

@timed()
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


# ---------------------------------------------------------------------------
# Media 判斷相關
# ---------------------------------------------------------------------------

MEDIA_TYPE_MAPPING = {
    # image
    'jpg': 'image', 'jpeg': 'image', 'png': 'image', 'gif': 'image',
    'webp': 'image', 'bmp': 'image', 'svg': 'image', 'tif': 'image', 'tiff': 'image',
    # video
    'mp4': 'video', 'mov': 'video', 'avi': 'video', 'mkv': 'video',
    'webm': 'video', 'flv': 'video', 'wmv': 'video',
    # audio
    'mp3': 'audio', 'wav': 'audio', 'ogg': 'audio', 'flac': 'audio',
    'm4a': 'audio', 'aac': 'audio', 'wma': 'audio',
}


def _get_media_extension(media_url):
    """
    從 URL 取出副檔名 (lowercase, 不含點)。
    支援 query string (?xxx) 與 fragment (#xxx)，無法判斷時回傳 ''。
    """
    if not media_url:
        return ''
    url = media_url.split('?', 1)[0].split('#', 1)[0]
    last_segment = url.rsplit('/', 1)[-1]
    if '.' not in last_segment:
        return ''
    ext = last_segment.rsplit('.', 1)[1].lower()
    # 防呆: 太長或含特殊字元的視為無效
    if not ext or len(ext) > 5 or not ext.isalnum():
        return ''
    return ext


def _get_media_type(media_url):
    """從 URL 副檔名推斷類別 (image / video / audio)；無法判斷回傳 'unknown'"""
    ext = _get_media_extension(media_url)
    return MEDIA_TYPE_MAPPING.get(ext, 'unknown')


def _compute_media_types(media_str):
    """
    從 '|' 或 ';' 分隔的 URL 字串對應出 ';' 分隔的類別字串。
    每個 URL 對應一個類別 (image/video/audio/unknown)，位置與原 URL 對齊。
    (僅用副檔名判斷；需要 HEAD fallback 請改用 resolve_media_types + _compute_media_types_with_map)
    """
    if not media_str:
        return ''
    types = []
    for url in re.split(r'[|;]', media_str):
        url = url.strip()
        types.append(_get_media_type(url) if url else 'unknown')
    return ';'.join(types)


# --- 探測 fallback：副檔名判不出來時實際發請求 --------------------------------

# domain -> (media_type, media_rules) 快取，跨 batch / 同次執行共用，
# 避免對同來源重複打 HEAD。
_media_type_cache = {}
_media_type_cache_lock = threading.Lock()

# 打 HEAD 時帶個 UA，避免部分伺服器擋掉預設的 python-requests
_MEDIA_HEAD_HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; TBIA-media-bot)'}


def _get_domain(media_url):
    """取出 domain（小寫），失敗回 ''。"""
    try:
        return urlparse(media_url).netloc.lower()
    except Exception:
        return ''


def _split_urls(media_str):
    """把 '|' 或 ';' 分隔的字串切成乾淨的 URL list。"""
    return [u.strip() for u in re.split(r'[|;]', media_str) if u.strip()]


def _url_to_rule(url):
    """把 URL 轉成 media_rule 前綴 (scheme://host)，失敗回 None。"""
    try:
        parts = urlparse(url)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}"
    except Exception:
        pass
    return None


def _probe_media(media_url, timeout=5):
    """
    對單一 URL 發請求，一次取得兩件事：
      1. media type（依最終回應的 Content-Type）
      2. 轉址鏈上所有 host 的 media_rule（含來源與最終目標）
    HEAD 不支援 (4xx/5xx 或無 Content-Type) 時退成 GET Range: bytes=0-0。
    失敗回 ('unknown', set())。
    """
    try:
        resp = requests.head(media_url, allow_redirects=True,
                             timeout=timeout, headers=_MEDIA_HEAD_HEADERS)
        if resp.status_code >= 400 or not resp.headers.get('Content-Type'):
            resp = requests.get(media_url, allow_redirects=True, timeout=timeout,
                                headers={**_MEDIA_HEAD_HEADERS, 'Range': 'bytes=0-0'})
    except Exception:
        return 'unknown', set()

    # resp.history 是導向過程的每個回應，resp 是最終；逐一取 host
    rules = set()
    for r in list(resp.history) + [resp]:
        rule = _url_to_rule(r.url)
        if rule:
            rules.add(rule)

    content_type = (resp.headers.get('Content-Type') or '').split(';')[0].strip().lower()
    media_type = 'unknown'
    for prefix in ('image', 'video', 'audio'):
        if content_type.startswith(prefix + '/'):
            media_type = prefix
            break

    return media_type, rules


def resolve_media_types(urls, max_workers=10):
    """
    對一批 URL 解析 media type，並收集轉址鏈上的真實 host（非 wildcard）。
    回傳 (type_map, observed_rules)：
      type_map:       {url: 'image'|'video'|'audio'|'unknown'}
      observed_rules: set，探測實際觀察到的 scheme://host（含轉址目標）

    流程（選項 B：每個 domain 都探測一次）：
      1. 每個 URL 先用副檔名判 type（免連網），判得出來就直接用作 type。
      2. 不論副檔名判不判得出來，每個 domain 都抽一個代表 URL 探測一次，
         目的在確認轉址目標 host（轉址與有無副檔名無關）。
         - 探測回傳的 type 只在副檔名判不出來時採用。
         - 探測回傳的 host 一律併入 observed_rules。
    以 domain 為快取 / 去重單位，假設同來源型別與轉址目標一致
    （生多媒體來源通常成立）；per-domain 只探一次，成本可控。
    """
    type_map = {}
    observed_rules = set()
    ext_type = {}       # url -> 副檔名判出的 type（判不出為 'unknown'）
    pending = {}        # domain -> 代表 url（每個 domain 都要探一次）

    for url in urls:
        if not url or url in ext_type:
            continue
        ext_type[url] = _get_media_type(url)  # 副檔名快路徑
        domain = _get_domain(url)
        with _media_type_cache_lock:
            cached = _media_type_cache.get(domain)
        if cached is not None:
            observed_rules.update(cached[1])
        else:
            pending.setdefault(domain, url)

    if pending:
        def worker(item):
            domain, sample_url = item
            t, rules = _probe_media(sample_url)
            return domain, t, rules
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for domain, t, rules in ex.map(worker, pending.items()):
                with _media_type_cache_lock:
                    _media_type_cache[domain] = (t, rules)
                observed_rules.update(rules)

    # 決定每個 url 最終 type：副檔名優先，判不出來才用該 domain 的探測結果
    for url in urls:
        if not url or url in type_map:
            continue
        t = ext_type.get(url, 'unknown')
        if t != 'unknown':
            type_map[url] = t          # 副檔名判得出來，直接採用
        else:
            with _media_type_cache_lock:
                cached = _media_type_cache.get(_get_domain(url))
            type_map[url] = cached[0] if cached else 'unknown'

    return type_map, observed_rules


def _compute_media_types_with_map(media_str, type_map):
    """用預先解析好的 type_map 組出 ';' 分隔的型別字串，位置與 URL 對齊。"""
    if not media_str:
        return ''
    return ';'.join(type_map.get(u, 'unknown') for u in _split_urls(media_str))


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
    從 '|' 或 ';' 分隔的 URL 字串取出所有 media_rule (protocol+domain)。
    支援單一 URL 或多 URL；自動忽略空字串、保留出現順序、跨 URL 去重。
    """
    if not media_str:
        return []
    rules = []
    for url in re.split(r'[|;]', media_str):
        url = url.strip()
        if not url:
            continue
        rule = _get_media_rule(url)
        if rule and rule not in rules:
            rules.append(rule)
    return rules


def flatten_media(media_list):
    # asiz, fact的 media 
    if not isinstance(media_list, list):
        return '', ''
    urls = []
    licences = []
    for am in media_list:
        if am and am.get('licence'): 
            urls.append(am.get('url'))
            licences.append(am.get('licence'))
    return ';'.join(urls), ';'.join(licences)


def apply_media_rule(df, media_rule_list):
    """
    處理 mediaLicense + associatedMedia 區塊。
    若 mediaLicense 為空（或根本沒有該欄位）則清空 associatedMedia，並蒐集所有出現過的 media_rule。
    associatedMedia 支援 '|' 或 ';' 分隔多 URL，跨 domain 也能正確收集。

    media_rule 來源有兩類，都會併入：
      - 靜態抽出：從 associatedMedia 直接取來源 host（_extract_media_rules）
      - 探測觀察：副檔名判不出 type 時發請求，順便收集轉址鏈上的目標 host
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

    # 3. 批次解析 media type（副檔名 → 探測 fallback），同時取得轉址目標 host
    all_urls = set()
    for media_str in df['associatedMedia']:
        if media_str:
            all_urls.update(_split_urls(media_str))
    type_map, probed_rules = resolve_media_types(all_urls)
    df['associatedMediaType'] = df['associatedMedia'].apply(
        lambda s: _compute_media_types_with_map(s, type_map)
    )

    # 4. 收集 media_rule：靜態抽出的來源 host + 探測觀察到的轉址目標 host
    new_rules = set(probed_rules)
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


# deprecated
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


@timed()
def update_gbif_references(df, existed_records, max_workers=10):
    """
    更新 df 的 references 欄位為 GBIF occurrence URL。

    規則：
    1. 有 gbifID 的直接組 URL
    2. 沒有 gbifID 但 existed_records 中該 tbiaID 已有 references 的跳過
    3. 其餘呼叫 get_gbif_id 取得

    Args:
        df: 待更新的 DataFrame，需包含 id, gbifID, gbifDatasetID, occurrenceID 欄位
        existed_records: 已存在的記錄 DataFrame，可能包含 tbiaID, references 欄位
        max_workers: 平行呼叫 get_gbif_id 的 thread 數

    Returns:
        修改後的 df
    """
    # 1. 有 gbifID 的直接向量化處理
    mask_has_gbif = df['gbifID'].astype(bool)
    df.loc[mask_has_gbif, 'references'] = (
        'https://www.gbif.org/occurrence/' + df.loc[mask_has_gbif, 'gbifID'].astype(str)
    )

    # 2. 找出需要呼叫 API 的 rows
    if 'references' in existed_records.columns:
        ids_with_ref = set(
            existed_records.loc[existed_records['references'] != '', 'tbiaID']
        )
        need_api_mask = ~mask_has_gbif & ~df['id'].isin(ids_with_ref)
    else:
        need_api_mask = ~mask_has_gbif

    # 3. 平行呼叫 get_gbif_id
    need_api_df = df.loc[need_api_mask, ['gbifDatasetID', 'occurrenceID']]

    def fetch(idx_row):
        idx, row = idx_row
        return idx, get_gbif_id(row.gbifDatasetID, row.occurrenceID)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, gbif_id in executor.map(fetch, need_api_df.iterrows()):
            if gbif_id:
                df.loc[idx, 'references'] = f"https://www.gbif.org/occurrence/{gbif_id}"

    return df