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

# 來源資料庫名稱對照（舊 rightsHolder → 新 sourceDatabase，見交接文件 2.1）
# 各單位名稱定案前為空；檔案格式 {"舊名稱": "新名稱", ...}
SOURCE_DATABASE_MAP_PATH = os.getenv('SOURCE_DATABASE_MAP', '/portal/bucket/v2/source_database_map.json')


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


_source_database_map = None


def source_database_map():
    global _source_database_map
    if _source_database_map is None:
        try:
            with open(SOURCE_DATABASE_MAP_PATH, encoding='utf-8') as f:
                _source_database_map = json.load(f)
            print(f'[export] 來源資料庫名稱對照 {len(_source_database_map)} 筆（{SOURCE_DATABASE_MAP_PATH}）')
        except FileNotFoundError:
            _source_database_map = {}
    return _source_database_map


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


def _fmt_datetime(x):
    # update 腳本傳入的已是 'YYYY-MM-DD HH:MM:SS' 字串；指令列由 SQL to_char 產生同格式
    if x is None or x is pd.NaT or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.strftime('%Y-%m-%d %H:%M:%S')
    return x


def _to_int(series):
    num = pd.to_numeric(series, errors='coerce')
    num = num.where(np.isfinite(num) & (num == np.floor(num)))
    return num.astype('Int64')


def _to_float(series):
    num = pd.to_numeric(series, errors='coerce')
    return num.where(np.isfinite(num))


def to_v2(df, taxon_df=None):
    """records df（PG 欄位名，或 v1 CSV）＋ taxa df（taxa 欄位名）→ v2 欄位 df。"""
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
        # TODO 2.3：改成暫行文字，目前仍輸出 True / False
    if 'sourceDatabase' in out.columns and source_database_map():
        out['sourceDatabase'] = out['sourceDatabase'].replace(source_database_map())

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