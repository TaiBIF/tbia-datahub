"""
records → Solr tbia_records（v2 欄位）CSV 匯出

第一階段（D17）PG records 與 update 腳本維持舊欄位名稱，只在這裡把舊格式對應成 v2。
update 腳本呼叫 export_records_with_taxon(df, output_path)，介面不變，輸出為 v2 欄位。
上線前把既有 v1 CSV 轉成 v2 的一次性工具在 scripts/v2_migration/convert_v1_csv.py（共用 to_v2）。
"""
import json
import os
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from scripts.utils.progress import timed

SOLR_URL = os.getenv('SOLR_URL', 'http://solr:8983/solr')
TAXA_SELECT_URL = f'{SOLR_URL}/taxa/select'
SCHEMA_PATH = Path(__file__).resolve().parents[2] / 'solr-workspace/conf-tbia/tbia_records/managed-schema'

# ---------------------------------------------------------------------------
# 欄位對照表（舊名 → v2 名）
# ---------------------------------------------------------------------------

# PG records → v2
RECORDS_RENAME = {
    'tbiaID': 'id',                                   # Solr uniqueKey 仍為 id，API 輸出時才叫 tbiaID
    'eventDate': 'verbatimEventDate',
    'standardLatitude': 'decimalLatitude',
    'standardLongitude': 'decimalLongitude',
    'standardRawLatitude': 'rawDecimalLatitude',
    'standardRawLongitude': 'rawDecimalLongitude',
    'verbatimRawLatitude': 'rawVerbatimLatitude',
    'verbatimRawLongitude': 'rawVerbatimLongitude',
    'standardOrganismQuantity': 'organismQuantity',   # 原始字串 organismQuantity 先 drop（RECORDS_DROP）
    'sourceDatasetID': 'datasetID',
    'rightsHolder': 'sourceDatabase',
    'preservation': 'preparations',
    'originalVernacularName': 'verbatimIdentification',  # D5
    'originalScientificName': 'verbatimIdentification',  # v1 CSV 中的名稱（舊 export 改名過）
    'sourceScientificName': 'verbatimScientificName',
    'sourceVernacularName': 'verbatimVernacularName',
}

# 改名前先移除的 PG 欄位
RECORDS_DROP = [
    'organismQuantity',   # 原始字串不再進 Solr，v2 organismQuantity 來自 standardOrganismQuantity
    'scientificNameID',   # 來源提供值，改用 taxa taxon_name_id（D15）
]

# taxa core → v2（taxa core 本身維持舊名，D21）
TAXA_RENAME = {
    'taxon_name_id': 'scientificNameID',
    'name_author': 'scientificNameAuthorship',
    'common_name_c': 'vernacularName',
    'bioGroup': 'taxonGroup',
    'kingdom_c': 'kingdomChinese',
    'phylum_c': 'phylumChinese',
    'class_c': 'classChinese',
    'order_c': 'orderChinese',
    'family_c': 'familyChinese',
    'genus_c': 'genusChinese',
}

DATETIME_COLS = ['created', 'modified', 'standardDate', 'sourceCreated', 'sourceModified']
INT_COLS = ['year', 'month', 'day', 'individualCount']
FLOAT_COLS = ['decimalLatitude', 'decimalLongitude', 'rawDecimalLatitude', 'rawDecimalLongitude',
              'organismQuantity', 'coordinateUncertaintyInMeters', 'dataQuality', 'update_version']

# typeStatus 控制詞彙（D23）：key 為去掉括號註記、轉小寫後的值
TYPE_STATUS_MAP = {
    'holotype': 'HOLOTYPE', '正模標本': 'HOLOTYPE',
    'paratype': 'PARATYPE', '副模標本': 'PARATYPE',
    'isotype': 'ISOTYPE', '複模標本': 'ISOTYPE',
    'allotype': 'ALLOTYPE', '配模標本': 'ALLOTYPE',
    'syntype': 'SYNTYPE',
    'lectotype': 'LECTOTYPE', '選模標本': 'LECTOTYPE',
    'paralectotype': 'PARALECTOTYPE', '副選模標': 'PARALECTOTYPE',
    'neotype': 'NEOTYPE', '新模標本': 'NEOTYPE',
    'topotype': 'TOPOTYPE',
}

# 來源資料庫名稱對照（舊 rightsHolder → 新 sourceDatabase，11/12 定案，見交接文件最後一節）
# 只列有改名的；過渡期使用，P2-3 改由 Partner dbname 取得後與 RECORDS_RENAME 一起移除
SOURCE_DATABASE_MAP = {
    '中央研究院生物多樣性中心動物標本館': '中央研究院動物標本館 ASIZ',
    '中央研究院生物多樣性中心植物標本資料庫': '中央研究院植物標本館 HAST',
    '國家海洋資料庫及共享平台': '國家海洋資料庫及共享平臺',
    '國立海洋生物博物館生物典藏管理系統': '生物典藏管理系統',
    '科博典藏 (NMNS Collection)': '科博典藏',
    '國立臺灣博物館典藏': '國立臺灣博物館生物典藏資料',
    '愛自然-臺灣(iNaturalist Taiwan)': '愛自然-臺灣',
    '中油生態地圖': '中油生態保育資料庫',
}

# 2.2 衍生欄位的來源
PORTAL_PARTNER_API = os.getenv('PORTAL_PARTNER_API', 'https://tbiadata.tw/api/partner-info')
PARTNER_CACHE_PATH = os.getenv('PARTNER_CACHE', '/bucket/v2/partner_info.json')   # API 連不上時用上次的結果
COUNTY_SHP_PATH = '/bucket/TW_TOWN/TOWN_MOI_1131028.shp'                         # 與 geography.py 同一份
TAXA_VERSION = os.getenv('TAXA_VERSION')   # taxa 來源檔名日期 YYYYMMDD，例 20261030（D8）


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------

_schema_fields = None


def v2_schema_fields():
    """v2 managed-schema 中可直接寫入的欄位（排除 copyField 目的欄位與 _ 開頭的系統欄位）。"""
    global _schema_fields
    if _schema_fields is None:
        text = SCHEMA_PATH.read_text(encoding='utf-8')
        fields = set(re.findall(r'<field name="([^"]+)"', text))
        copy_dest = set(re.findall(r'<copyField [^>]*dest="([^"]+)"', text))
        _schema_fields = {f for f in fields - copy_dest if not f.startswith('_')}
    return _schema_fields


def source_database_map():
    return SOURCE_DATABASE_MAP


# ---------------------------------------------------------------------------
# taxa
# ---------------------------------------------------------------------------

_taxon_cache = {}


def fetch_taxon_df(taxon_ids, solr_url=TAXA_SELECT_URL, chunk_size=500, use_cache=False):
    """依 taxonID 列表從 taxa core 撈回分類資料（taxa 原欄位名）。use_cache 供指令列多批匯出重複利用。"""
    taxon_ids = [tid for tid in set(taxon_ids) if tid]
    if not taxon_ids:
        return pd.DataFrame()

    todo = [t for t in taxon_ids if t not in _taxon_cache] if use_cache else taxon_ids
    docs = []
    for i in range(0, len(todo), chunk_size):
        chunk = todo[i:i + chunk_size]
        query = {'query': '{!terms f=id}' + ','.join(chunk), 'limit': len(chunk)}
        r = requests.post(solr_url, data=json.dumps(query), headers={'content-type': 'application/json'})
        if r.status_code == 200:
            docs += r.json()['response']['docs']
        else:
            # 撈不到分類資料會讓整批缺分類欄位，直接中止比靜默輸出好
            raise RuntimeError(f'[fetch_taxon_df] solr error {r.status_code} at chunk {i}')

    if use_cache:
        for d in docs:
            _taxon_cache[d['id']] = d
        docs = [_taxon_cache[t] for t in taxon_ids if t in _taxon_cache]

    if not docs:
        return pd.DataFrame()

    taxon_df = pd.DataFrame(docs)
    taxon_df = taxon_df.drop(columns=['taxonID'], errors='ignore').rename(columns={'id': 'taxonID'})
    taxon_df = taxon_df[taxon_df.columns.drop(list(taxon_df.filter(regex='_taxonID$')))]
    # created / modified 等是 taxa 自己的時間，不能蓋掉 records 的
    taxon_df = taxon_df.drop(columns=['_version_', 'created', 'modified'], errors='ignore')
    taxon_df = taxon_df.replace({np.nan: None})
    return taxon_df


# ---------------------------------------------------------------------------
# 衍生欄位的來源（2.2）：每個程序只載入一次
# ---------------------------------------------------------------------------

_partner_id_map = None      # group → tbiaPartnerID
_county_code_map = None     # 縣市名稱 → 內政部 COUNTYCODE（D24）
_dataset_url_map = None     # tbiaDatasetID → datasetURL（D9 第一階段）
_unmapped_county = set()


def partner_id_map():
    """portal /api/partner-info：group → tbiaPartnerID（GBIF 為 null，D2）。連不上時用上次快取。"""
    global _partner_id_map
    if _partner_id_map is None:
        try:
            r = requests.get(PORTAL_PARTNER_API, timeout=30)
            r.raise_for_status()
            data = r.json()['data']
            try:
                os.makedirs(os.path.dirname(PARTNER_CACHE_PATH), exist_ok=True)
                with open(PARTNER_CACHE_PATH, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
            except OSError as e:
                print(f'[export] 無法寫入 partner 快取 {PARTNER_CACHE_PATH}: {e}')
        except Exception as e:
            if not os.path.exists(PARTNER_CACHE_PATH):
                raise RuntimeError(f'[export] 讀不到 {PORTAL_PARTNER_API}，也沒有快取：{e}')
            print(f'[export] 讀不到 {PORTAL_PARTNER_API}（{e}），改用快取 {PARTNER_CACHE_PATH}')
            with open(PARTNER_CACHE_PATH, encoding='utf-8') as f:
                data = json.load(f)
        _partner_id_map = {p['group']: p['tbiaPartnerID'] for p in data if p.get('group') and p.get('tbiaPartnerID')}
    return _partner_id_map


def county_code_map():
    global _county_code_map
    if _county_code_map is None:
        import geopandas as gpd
        t = gpd.read_file(COUNTY_SHP_PATH, ignore_geometry=True)[['COUNTYNAME', 'COUNTYCODE']].drop_duplicates()
        _county_code_map = dict(zip(t.COUNTYNAME, t.COUNTYCODE.astype(str)))
    return _county_code_map


def dataset_url_map():
    global _dataset_url_map
    if _dataset_url_map is None:
        import psycopg2
        from app import db_settings
        conn = psycopg2.connect(**db_settings)
        try:
            with conn.cursor() as cur:
                # 同一個 tbiaDatasetID 若有多列，以未 deprecated 的為準
                cur.execute('''SELECT "tbiaDatasetID", "datasetURL" FROM dataset
                               WHERE "tbiaDatasetID" <> '' AND "datasetURL" <> ''
                               ORDER BY deprecated DESC''')
                _dataset_url_map = {k: v for k, v in cur.fetchall()}
        finally:
            conn.close()
    return _dataset_url_map


def name_according_to():
    if not TAXA_VERSION or not re.fullmatch(r'\d{8}', TAXA_VERSION):
        raise RuntimeError('需要設定 TAXA_VERSION（taxa 來源檔名日期，例 20261030）')
    return f'臺灣物種名錄 {TAXA_VERSION}版本'


def set_taxa_version(v):
    global TAXA_VERSION
    TAXA_VERSION = v


def load_derived_sources():
    """平行轉檔前先在主程序載入，fork 出的子程序直接沿用。"""
    name_according_to()
    print(f'[export] partner {len(partner_id_map())} 個 group、縣市代碼 {len(county_code_map())} 筆、'
          f'dataset URL {len(dataset_url_map()):,} 筆、nameAccordingTo={name_according_to()}')


# eventTime（D6）：原始 eventDate 有時間才填，照原樣（含 00:00:00）；區間取第一個時間（起始）
_TIME_RE = re.compile(r'(?:T|\s)(上午|下午|AM|PM)?\s*(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?\s*(AM|PM)?\s*(Z|[+-]\d{2}:?\d{2})?',
                      re.IGNORECASE)


def event_time(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    m = _TIME_RE.search(str(value))   # 不可用 / 切區間：2020/01/01 這類日期本身就有 /
    if not m:
        return None
    ampm = (m.group(1) or m.group(5) or '').upper()
    h, mi, s, tz = int(m.group(2)), m.group(3), m.group(4), m.group(6) or ''
    if ampm in ('下午', 'PM') and h < 12:
        h += 12
    elif ampm in ('上午', 'AM') and h == 12:
        h = 0
    if h > 23 or int(mi) > 59 or (s and int(s) > 59):
        return None
    return f'{h:02d}:{mi}' + (f':{s}' if s else '') + tz


def add_derived(out):
    """2.2 衍生欄位：tbiaPartnerID、geodeticDatum、countyCode / rawCountyCode、countryCode、eventTime、
    resourceCitationIdentifier、nameAccordingTo（scientificNameID 已在 taxa 改名處理，D15）。"""
    if 'group' in out.columns:
        out['tbiaPartnerID'] = out['group'].map(partner_id_map())          # GBIF 不在對照中 → 空（D2）

    if {'decimalLatitude', 'decimalLongitude'} <= set(out.columns):
        has_coor = out['decimalLatitude'].notna() & out['decimalLongitude'].notna()
        out['geodeticDatum'] = np.where(has_coor, 'EPSG:4326', None)       # D14

    codes = county_code_map()
    for src, dst in (('county', 'countyCode'), ('rawCounty', 'rawCountyCode')):
        if src in out.columns:
            out[dst] = out[src].map(codes)
            miss = set(out.loc[out[src].notna() & out[dst].isna(), src].unique()) - _unmapped_county
            if miss:
                _unmapped_county.update(miss)
                print(f'[to_v2] 對不到縣市代碼的名稱：{sorted(miss)}')

    # D13：TBIA 推算在台灣範圍內（陸地或海域縣市有值）→ TW；records 沒有保存來源 countryCode，其餘留空
    in_tw = pd.Series(False, index=out.index)
    for c in ('county', 'rawCounty'):
        if c in out.columns:
            in_tw |= out[c].notna()
    out['countryCode'] = np.where(in_tw, 'TW', None)

    if 'verbatimEventDate' in out.columns:
        out['eventTime'] = out['verbatimEventDate'].map(event_time)

    if 'tbiaDatasetID' in out.columns:
        out['resourceCitationIdentifier'] = out['tbiaDatasetID'].map(dataset_url_map())

    out['nameAccordingTo'] = np.where(out['taxonID'].notna(), name_according_to(), None) \
        if 'taxonID' in out.columns else None
    return out


# ---------------------------------------------------------------------------
# 值的轉換
# ---------------------------------------------------------------------------

def normalize_type_status(value):
    """D23：以 ; 或 | 切段，對得到的轉 v2 詞彙，對不到保留原段落；以 | 合併並去重。"""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    out = []
    for seg in re.split(r'[;|]', str(value)):
        seg = seg.strip()
        if not seg:
            continue
        key = re.sub(r'[（(].*?[)）]', '', seg).strip().lower()
        v = TYPE_STATUS_MAP.get(key, seg)
        if v not in out:
            out.append(v)
    return '|'.join(out) if out else None


# 2.3 暫行文字（D4；正式句型 P2-5）：只描述 TBIA 統一模糊化的紀錄，來源端自行模糊化的不填
DG_PREFIX = '由 TBIA 統一模糊化'


def _precision_text(cp):
    """coordinatePrecision → 模糊化程度；與 geography._blur_coordinate 的規則一致，未模糊化回 None。"""
    cp = _parse_float(cp)
    if not np.isfinite(cp):
        return None
    if 0 < cp < 1:
        return f'座標取至小數點後 {round(-np.log10(cp))} 位'
    if cp == 1:
        return '座標取至整數位'
    return None


def data_generalizations_text(out):
    """依資料狀態產生 dataGeneralizations 文字（須在 RECORDS_RENAME 之後呼叫）。
    raw 座標只在 TBIA 模糊化時才有值（skip_blur 的來源不存 raw），以此判斷是否由 TBIA 處理：
      raw 有值、公開座標為空 → 座標不公開（taif、敏感層級「縣市」「座標不開放」）
      raw 有值、公開座標有值 → 依 coordinatePrecision 說明位數
      其餘 → 空值"""
    raw_cols = [c for c in ('rawDecimalLatitude', 'rawVerbatimLatitude') if c in out.columns]
    if not raw_cols:
        return pd.Series(None, index=out.index, dtype=object)
    has_raw = pd.Series(False, index=out.index)
    for c in raw_cols:
        has_raw |= out[c].notna() & (out[c].astype(str).str.strip() != '')
    public = out['decimalLatitude'].notna() if 'decimalLatitude' in out.columns else pd.Series(False, index=out.index)

    text = pd.Series(None, index=out.index, dtype=object)
    text[has_raw & ~public] = f'{DG_PREFIX}，座標不公開'
    blurred = has_raw & public
    if blurred.any() and 'coordinatePrecision' in out.columns:
        degree = out.loc[blurred, 'coordinatePrecision'].map(_precision_text)
        text[degree[degree.notna()].index] = DG_PREFIX + '，' + degree.dropna()
    return text


def _fmt_datetime(x):
    # update 腳本傳入的已是 'YYYY-MM-DD HH:MM:SS' 字串；指令列由 SQL to_char 產生同格式
    if x is None or x is pd.NaT or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.strftime('%Y-%m-%d %H:%M:%S')
    return x


def _parse_float(x):
    # 不用 pd.to_numeric：它解析字串不保證精確捨入，會差 1 ulp（23.649526841608566 → ...857）
    try:
        return float(x)
    except (TypeError, ValueError):
        return np.nan


def _to_float(series):
    num = series.map(_parse_float).astype('float64')
    return num.where(np.isfinite(num))


def _to_int(series):
    num = _to_float(series)
    return num.where(num == np.floor(num)).astype('Int64')


def to_v2(df, taxon_df=None, derive=True):
    """records df（PG 欄位名，或 v1 CSV）＋ taxa df（taxa 欄位名）→ v2 欄位 df。derive=False 不產生 2.2 衍生欄位。"""
    out = df.drop(columns=[c for c in RECORDS_DROP if c in df.columns])
    out = out.drop(columns=['id'], errors='ignore') if 'tbiaID' in out.columns else out  # PG 流水號
    out = out.rename(columns=RECORDS_RENAME)
    out = out.rename(columns=TAXA_RENAME)   # v1 CSV 已 merge 過 taxa（舊名）；PG 來源沒有這些欄位

    if taxon_df is not None and len(taxon_df):
        taxon_df = taxon_df.rename(columns=TAXA_RENAME)
        # 同名欄位以 records 為準（taxonID 為 merge key）
        dup = [c for c in taxon_df.columns if c in out.columns and c != 'taxonID']
        taxon_df = taxon_df.drop(columns=dup)
        n = len(out)
        out = out.merge(taxon_df, on='taxonID', how='left')
        if len(out) != n:
            print(f'[to_v2] merge row count mismatch: {n} -> {len(out)}')

    for c in DATETIME_COLS:
        if c in out.columns:
            out[c] = out[c].map(_fmt_datetime)
    for c in INT_COLS:
        if c in out.columns:
            out[c] = _to_int(out[c])
    for c in FLOAT_COLS:
        if c in out.columns:
            out[c] = _to_float(out[c])

    if 'coordinatePrecision' in out.columns:
        # 舊資料有大量字串 'NaN'（交接紀錄 11/6 備忘）
        out['coordinatePrecision'] = out['coordinatePrecision'].replace({'NaN': None, 'nan': None, '': None})
    if 'typeStatus' in out.columns:
        out['typeStatus'] = out['typeStatus'].map(normalize_type_status)
    if 'dataGeneralizations' in out.columns:
        # 模糊化的紀錄 verbatim* 為模糊後的值，v2 公開欄位留空，原值在 rawVerbatim*
        # PG 來源為 bool、v1 CSV 來源為字串 'True' / 'False'
        blurred = out['dataGeneralizations'].map(lambda x: str(x).strip().lower() in ('true', '1', 't'))
        for c in ['verbatimLatitude', 'verbatimLongitude']:
            if c in out.columns:
                out.loc[blurred, c] = None
        out['dataGeneralizations'] = data_generalizations_text(out)
    if 'sourceDatabase' in out.columns and source_database_map():
        out['sourceDatabase'] = out['sourceDatabase'].replace(source_database_map())
    if derive:
        out = add_derived(out)

    # 只輸出 v2 schema 有的欄位，避免 schemaless 自動建出多值欄位
    allowed = v2_schema_fields()
    extra = [c for c in out.columns if c not in allowed]
    if extra:
        print(f'[to_v2] 不在 v2 schema、未輸出的欄位：{sorted(extra)}')
        out = out.drop(columns=extra)
    return out


@timed()
def export_records_with_taxon(df, output_path):
    """把 records df（prepare_df_for_sql 後）與 taxa 合併，以 v2 欄位輸出 CSV。"""
    taxon_df = fetch_taxon_df(df['taxonID'].dropna().tolist())
    to_v2(df, taxon_df).to_csv(output_path, index=None)