"""
records → Solr tbia_records（v2 欄位）CSV 匯出

第一階段（D17）PG records 與 update 腳本維持舊欄位名稱，只在這裡把舊格式對應成 v2：
  - export_records_with_taxon(df, output_path)：update 腳本呼叫，介面不變，輸出改為 v2 欄位
  - 指令列：從 PG 直接匯出指定單位（測試站 v2 試匯出 / 2.6 全量匯出用），可選擇匯入 core 並與 v1 比對

使用範例（測試站在 ~/tbia-datahub-v2，一律加 -f）：
  # 列出各單位筆數，挑試匯出對象
  docker compose -f docker-compose.v2.yml exec app-v2 python -m scripts.utils.export --list

  # 只輸出 CSV（預設寫到 /tmp/export，對應主機 /tmp/export_v2）
  docker compose -f docker-compose.v2.yml exec app-v2 python -m scripts.utils.export --group oca

  # 輸出 + 清掉 core 內該單位舊資料後匯入 tbia_records_new + 抽 200 筆與 v1 比對
  docker compose -f docker-compose.v2.yml exec app-v2 python -m scripts.utils.export \
      --group oca --overwrite --post --replace --compare 200
"""
import argparse
import glob
import json
import os
import re
import sys
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
    'originalVernacularName': 'verbatimIdentification',  # v1 匯出時叫 originalScientificName（D5）
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
    """records df（PG 欄位名）＋ taxa df（taxa 欄位名）→ v2 欄位 df。"""
    out = df.drop(columns=[c for c in RECORDS_DROP if c in df.columns])
    out = out.drop(columns=['id'], errors='ignore') if 'tbiaID' in out.columns else out  # PG 流水號
    out = out.rename(columns=RECORDS_RENAME)

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
        blurred = out['dataGeneralizations'].fillna(False).astype(bool)
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


# ---------------------------------------------------------------------------
# 指令列：從 PG 匯出 / 匯入 core / 與 v1 比對
# ---------------------------------------------------------------------------

def _pg_conn():
    import psycopg2
    from app import db_settings
    return psycopg2.connect(**db_settings)


def list_sources():
    sql = 'SELECT "group", "rightsHolder", count(*) FROM records GROUP BY 1, 2 ORDER BY 3'
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for g, rh, n in cur.fetchall():
            print(f'{n:>12,}  {g:<12} {rh}')


def _select_sql(cur):
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'records' ORDER BY ordinal_position")
    cols = [r[0] for r in cur.fetchall() if r[0] != 'id']
    parts = []
    for c in cols:
        if c in DATETIME_COLS:
            # 依連線時區格式化，與 update 時 naive 時間寫入的解讀一致
            parts.append(f'''to_char("{c}", 'YYYY-MM-DD HH24:MI:SS') AS "{c}"''')
        else:
            parts.append(f'"{c}"')
    return 'SELECT ' + ', '.join(parts) + ' FROM records'


def export_from_pg(group, rights_holder, out_dir, chunk_size, limit=None):
    """依 group（+ rightsHolder）從 PG 分批讀出，轉 v2 後每批一個 CSV。回傳 (檔案清單, 筆數)。"""
    where = ['"group" = %s', 'is_deleted IS NOT TRUE']
    params = [group]
    if rights_holder:
        where.append('"rightsHolder" = %s')
        params.append(rights_holder)

    files, total = [], 0
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            base = _select_sql(cur)
        sql = base + ' WHERE ' + ' AND '.join(where) + ' ORDER BY "tbiaID"'
        if limit:
            sql += f' LIMIT {int(limit)}'
        with conn.cursor(name='v2_export') as cur:   # server-side cursor，不一次載入
            cur.itersize = chunk_size
            cur.execute(sql, params)
            cols = None
            n = 0
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                if cols is None:
                    cols = [d[0] for d in cur.description]
                df = pd.DataFrame(rows, columns=cols)
                taxon_df = fetch_taxon_df(df['taxonID'].dropna().tolist(), use_cache=True)
                path = os.path.join(out_dir, f'{group}_{n:05d}.csv')
                to_v2(df, taxon_df).to_csv(path, index=None)
                files.append(path)
                total += len(df)
                n += 1
                print(f'[export] {path}  累計 {total:,} 筆')
    return files, total


def solr_count(core, q):
    r = requests.get(f'{SOLR_URL}/{core}/select', params={'q': q, 'rows': 0, 'wt': 'json'})
    r.raise_for_status()
    return r.json()['response']['numFound']


def _q(field, value):
    return f'{field}:"{value}"'


def post_files(files, core, replace_q=None):
    if replace_q:
        print(f'[post] 刪除 {core} 中 {replace_q}：{solr_count(core, replace_q):,} 筆')
        r = requests.post(f'{SOLR_URL}/{core}/update', params={'commit': 'true'},
                          json={'delete': {'query': replace_q}})
        r.raise_for_status()
    failed = []
    for f in files:
        with open(f, 'rb') as fh:
            r = requests.post(f'{SOLR_URL}/{core}/update', params={'commit': 'false'},
                              data=fh, headers={'Content-Type': 'text/csv; charset=utf-8'})
        if r.status_code != 200:
            failed.append(f)
            print(f'[post] 失敗 {f}: {r.status_code} {r.text[:500]}')
    requests.get(f'{SOLR_URL}/{core}/update', params={'commit': 'true', 'openSearcher': 'true'}).raise_for_status()
    print(f'[post] 匯入 {len(files) - len(failed)} / {len(files)} 個檔案')
    return failed


# v1 欄位 → v2 欄位（比對用）
V1_TO_V2 = {**{('originalScientificName' if k == 'originalVernacularName' else k): v
               for k, v in RECORDS_RENAME.items() if k != 'tbiaID'},
            **{k: v for k, v in TAXA_RENAME.items() if k != 'taxon_name_id'}}
# 規則本來就會不同的欄位：比對時只計數不列為異常
EXPECTED_DIFF = {'typeStatus', 'verbatimLatitude', 'verbatimLongitude', 'dataGeneralizations',
                 'scientificNameID', 'organismQuantity', 'sourceDatabase',
                 'coordinatePrecision', 'coordinateUncertaintyInMeters'}   # 字串 NaN / 非數字轉空值


def _norm(v):
    if isinstance(v, list):
        v = v[0] if len(v) == 1 else '|'.join(map(str, v))
    if v is None or v == '':
        return None
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v)
    if re.fullmatch(r'-?\d+(\.\d+)?', s):
        return float(s)
    if s.lower() in ('true', 'false'):
        return s.lower()
    return s


def compare_with_v1(v1_core, v2_core, v1_q, v2_q, sample):
    n1, n2 = solr_count(v1_core, v1_q), solr_count(v2_core, v2_q)
    print(f'\n[compare] 筆數 {v1_core}={n1:,}  {v2_core}={n2:,}  差 {n2 - n1:+,}')

    r = requests.get(f'{SOLR_URL}/{v2_core}/select',
                     params={'q': v2_q, 'rows': sample, 'sort': f'random_{datetime.now():%H%M%S} asc', 'wt': 'json'})
    r.raise_for_status()
    new_docs = {d['id']: d for d in r.json()['response']['docs']}
    if not new_docs:
        print('[compare] v2 core 沒有資料')
        return
    ids = list(new_docs)
    r = requests.post(f'{SOLR_URL}/{v1_core}/select',
                      json={'query': '{!terms f=id}' + ','.join(ids), 'limit': len(ids)})
    r.raise_for_status()
    old_docs = {d['id']: d for d in r.json()['response']['docs']}
    print(f'[compare] 抽樣 {len(ids)} 筆，v1 找到 {len(old_docs)} 筆')

    diff, examples = {}, {}
    for i in ids:
        old, new = old_docs.get(i), new_docs[i]
        if old is None:
            continue
        for k1, val in old.items():
            if k1.startswith('_') or k1 == 'originalVernacularName':
                continue
            k2 = V1_TO_V2.get(k1, k1)
            if k1 == 'organismQuantity':      # v1 原始字串，v2 已不存
                continue
            if k1 == 'standardOrganismQuantity':
                k2 = 'organismQuantity'
            a, b = _norm(val), _norm(new.get(k2))
            if a != b:
                diff[k2] = diff.get(k2, 0) + 1
                examples.setdefault(k2, (i, val, new.get(k2)))
        for k2 in new:
            if k2 not in V1_TO_V2.values() and k2 not in old and not k2.startswith('_') \
                    and new[k2] not in (None, ''):
                diff[f'{k2}（v2 新增）'] = diff.get(f'{k2}（v2 新增）', 0) + 1

    if not diff:
        print('[compare] 所有欄位一致')
    for k in sorted(diff, key=lambda x: (x.split('（')[0] in EXPECTED_DIFF or '新增' in x, x)):
        tag = '預期' if (k in EXPECTED_DIFF or '新增' in k) else '異常'
        ex = examples.get(k)
        ex_s = f'  例 {ex[0]}: {ex[1]!r} → {ex[2]!r}' if ex else ''
        print(f'  [{tag}] {k}: {diff[k]} 筆{ex_s}')


def main(argv=None):
    p = argparse.ArgumentParser(description='PG records → v2 CSV（可匯入 core 並與 v1 比對）')
    p.add_argument('--list', action='store_true', help='列出各單位筆數')
    p.add_argument('--group')
    p.add_argument('--rights-holder', help='同 group 有多個資料庫時指定（舊名稱）')
    p.add_argument('--out', default='/tmp/export')
    p.add_argument('--chunk', type=int, default=20000, help='每個 CSV 的筆數')
    p.add_argument('--limit', type=int, help='只匯出前 N 筆（測試用）')
    p.add_argument('--overwrite', action='store_true', help='先刪除 out 目錄中同 group 的舊 CSV')
    p.add_argument('--post', action='store_true', help='匯出後匯入 --core')
    p.add_argument('--core', default='tbia_records_new')
    p.add_argument('--replace', action='store_true', help='匯入前刪除 core 中該單位的資料')
    p.add_argument('--compare', type=int, default=0, metavar='N', help='與 v1 core 比對筆數並抽 N 筆比欄位')
    p.add_argument('--v1-core', default='tbia_records')
    a = p.parse_args(argv)

    if a.list:
        list_sources()
        return
    if not a.group:
        p.error('需要 --group（或 --list）')
    if a.core in ('tbia_records', 'tbia_records_build', 'tbia_records_v1') and (a.post or a.replace):
        sys.exit(f'拒絕寫入 {a.core}：開發期只寫 tbia_records_new，正式灌入走 import_and_swap_data.sh')

    os.makedirs(a.out, exist_ok=True)
    old = glob.glob(os.path.join(a.out, f'{a.group}_*.csv'))
    if old:
        if not a.overwrite:
            sys.exit(f'{a.out} 已有 {len(old)} 個 {a.group}_*.csv，加 --overwrite 先刪除')
        for f in old:
            os.remove(f)

    files, total = export_from_pg(a.group, a.rights_holder, a.out, a.chunk, a.limit)
    print(f'[export] 完成：{total:,} 筆、{len(files)} 個檔案')

    v1_q = _q('group', a.group) + (f' AND {_q("rightsHolder", a.rights_holder)}' if a.rights_holder else '')
    new_rh = source_database_map().get(a.rights_holder, a.rights_holder) if a.rights_holder else None
    v2_q = _q('group', a.group) + (f' AND {_q("sourceDatabase", new_rh)}' if new_rh else '')

    if a.post and files:
        post_files(files, a.core, replace_q=v2_q if a.replace else None)
    if a.compare:
        compare_with_v1(a.v1_core, a.core, v1_q, v2_q, a.compare)


if __name__ == '__main__':
    main()