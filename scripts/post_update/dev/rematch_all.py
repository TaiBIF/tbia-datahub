"""
全資料庫重新比對學名（不經過來源 API）

流程：
0. 建立工作表；找出「來源已提供 taxonID」且該 taxonID 未被 TaiCOL 刪除的資料
   （match_log.match_stage = 0），這些資料不重新比對
1. psql 單次掃描取出各比對設定的不重複學名組合 → 分批比對 TaiCOL（pkl 快取，可中斷續跑）
2. 比對結果寫入 _rematch_names
3. 依單位、依 tbiaID 範圍分批更新：
   records   ：只更新比對結果有變動的資料（taxonID、match_higher_taxon、modified）
   match_log ：比對欄位 + modified（有差異才改），缺少的補 INSERT
4. 輸出全部 records 成 Solr 匯入 CSV（多行程平行）
5. 重建各單位 match_log zip

records 只會動到 taxonID、match_higher_taxon、modified 三個欄位。
⚠️ 沒變動的資料 modified 不會更新，Solr 匯入請用 ACTION=import，不要執行 delete。
⚠️ 比對程式有改時，執行前請先清空 /code/rematch_cache/，否則會沿用舊的比對結果。
不會新增 update_version、不會刪除任何資料。
除第 1 步有快取外，其他步驟重跑都是冪等的，中斷後直接重跑即可。

用法（單位格式 {group}_{info_id}）：
  python rematch_all.py                                 # 全部比對、全部更新
  python rematch_all.py --only nps_0 nps_1              # 只比對、更新這些單位（換範圍要清快取）
  python rematch_all.py --update-only nps_0 nps_1       # 全部比對（可沿用快取），只更新這些單位
"""
import argparse
import io
import os
import sys
import glob
import time
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from sqlalchemy import text
from tqdm import tqdm
from app import engine
from scripts.utils.records import records_cols
from scripts.utils.match import (matching_flow_new_optimized, create_match_log_df, zip_match_log,
                                 match_cols, deleted_taxon_ids)
from scripts.utils.export import fetch_taxon_df

# ── 比對欄位 ──
SS, SV, OV, SID = 'sourceScientificName', 'sourceVernacularName', 'originalVernacularName', 'scientificNameID'
FAM, ORD, CLS, KIN = 'sourceFamily', 'sourceOrder', 'sourceClass', 'sourceKingdom'
NAME_COLS = [SS, SV, OV, SID, FAM, ORD, CLS, KIN]

# ── 各單位設定：取自 scripts/update/*.py 的 sci_cols ──
# taxonID（來源提供，另外處理）與 sourceTaxonID（比對流程不使用）已排除
HOLDERS = [
    # (rights_holder, group, info_id, 比對欄位)
    ('集水區友善環境生態資料庫',                'ardswc',      0, [SS, SV, FAM, ORD, CLS, KIN]),
    ('臺灣魚類資料庫',                          'ascdc',       0, [SS, SV]),
    ('中央研究院生物多樣性中心動物標本館',        'asiz',        0, [SS, SV, FAM]),
    ('中油生態地圖',                            'cpc',         0, [SV, SS, OV, CLS, ORD, FAM, KIN]),
    ('林業試驗所昆蟲標本館',                     'fact',        0, [SS, SV]),
    ('生態調查資料庫系統',                       'forest',      0, [SS, SV]),
    ('GBIF',                                   'gbif',        0, [SV, SS, OV, CLS, ORD, FAM, KIN]),
    ('中央研究院生物多樣性中心植物標本資料庫',     'hast',        0, [SS, SV, FAM, KIN]),
    ('愛自然-臺灣(iNaturalist Taiwan)',         'ntuforestry', 0, [SS, SV, CLS, KIN]),
    ('國立海洋生物博物館生物典藏管理系統',        'nmmba',       0, [SS, SV, FAM]),
    ('科博典藏 (NMNS Collection)',              'nmns',        0, [SS, SV, ORD, FAM, CLS, KIN]),
    ('國家海洋資料庫及共享平台',                  'namr',        0, [SS, SV, CLS, ORD, FAM, KIN]),
    ('作物種原資訊系統',                         'npgrc',       0, [SS, SV, FAM]),
    ('臺灣國家公園生物多樣性資料庫',              'nps',         0, [SS, SV]),
    ('國立臺灣博物館典藏',                       'ntm',         0, [SV, SS, OV, CLS, ORD, FAM, KIN]),
    ('海洋保育資料倉儲系統',                     'oca',         0, [SV, SS, SID, CLS, ORD, FAM]),
    ('臺灣生物多樣性資訊機構 TaiBIF',             'brcas',       0, [SV, SS, OV, CLS, ORD, FAM, KIN]),
    ('林業試驗所植物標本資料庫',                  'taif',        0, [SS, SV, KIN]),
    ('台灣生物多樣性網絡 TBN',                   'tbri',        0, [SS, SV, OV, FAM, KIN]),
    ('濕地環境資料庫',                           'nps',         1, [SS, SV]),
    ('河川環境資料庫',                           'wra',         0, [SV, SS, OV, CLS, ORD, FAM, KIN]),
]
# sci_cols 含 taxonID（來源會提供 TaiCOL taxonID）的單位
SRC_TAXON_HOLDERS = {'集水區友善環境生態資料庫', '中油生態地圖', 'GBIF', '國立臺灣博物館典藏', '海洋保育資料倉儲系統',
                     '臺灣生物多樣性資訊機構 TaiBIF', '台灣生物多樣性網絡 TBN', '濕地環境資料庫', '河川環境資料庫'}
# ── 執行範圍（命令列參數，格式 {group}_{info_id}，同 match_report 的 --unit）──
#   --only         比對範圍；省略 = 全部。⚠️ 會改變 cfg 編號，換範圍時要清空 CACHE_DIR
#   --update-only  第 3~5 步（更新 records/match_log、輸出 Solr CSV、重建 zip）只處理這些單位；
#                  省略 = 同 --only。只改這個不影響 cfg 編號，比對快取（cfg_*.pkl）可沿用
_ap = argparse.ArgumentParser(description='全資料庫重新比對學名')
_ap.add_argument('--only', nargs='+', default=[], metavar='UNIT', help='比對範圍，如 nps_0 nps_1')
_ap.add_argument('--update-only', nargs='+', default=[], metavar='UNIT', help='只更新這些單位')
args, _ = _ap.parse_known_args()

unit_of = lambda h: f'{h[1]}_{h[2]}'
_unknown = set(args.only + args.update_only) - {unit_of(h) for h in HOLDERS}
if _unknown:
    sys.exit(f'HOLDERS 中沒有這些單位：{", ".join(sorted(_unknown))}')
ONLY = [h[0] for h in HOLDERS if unit_of(h) in args.only]
UPDATE_ONLY = [h[0] for h in HOLDERS if unit_of(h) in args.update_only]
if UPDATE_ONLY and ONLY and set(UPDATE_ONLY) - set(ONLY):
    sys.exit('--update-only 的單位必須包含在 --only 範圍內')

# ── 參數 ──
BATCH = 200000          # records / match_log 每批 commit 筆數
NAME_CHUNK = 2000       # TaiCOL 每批比對的學名組合數
MATCH_WORKERS = 4       # TaiCOL API 並行數（API 撐得住可調高）
EXPORT_CHUNK = 50000    # Solr CSV 每檔筆數
EXPORT_WORKERS = 4      # 輸出 CSV 的平行行程數
CACHE_DIR = '/code/rematch_cache'
EXPORT_DIR = '/solr/csvs/export'
now = datetime.now() + timedelta(hours=8)
now_str = now.strftime('%Y-%m-%d %H:%M:%S')

match_out_cols = [c for c in match_cols if c != 'sci_index']
stage_cols = [f'stage_{i}' for i in range(1, 9)]
ml_value_cols = ['taxonID', 'match_higher_taxon', 'match_higher_partial', 'match_stage'] + stage_cols + ['is_matched']
rec_match_cols = ['taxonID', 'match_higher_taxon']      # records 中屬於比對結果的欄位

holders = [h for h in HOLDERS if not ONLY or h[0] in ONLY]
holder_names = [h[0] for h in holders]
update_holders = [h for h in holders if not UPDATE_ONLY or h[0] in UPDATE_ONLY]
update_names = [h[0] for h in update_holders]
# 比對欄位相同的單位共用一組（欄位「有沒有傳入」會影響比對邏輯，所以不能混在一起）
cfg_ids, cfg_cols = {}, {}
for _, _, _, cols in holders:
    key = tuple(sorted(cols))
    if key not in cfg_ids:
        cfg_ids[key] = len(cfg_ids)
        cfg_cols[cfg_ids[key]] = cols
cfg_of = lambda cols: cfg_ids[tuple(sorted(cols))]

# ── records 欄位型態（排除整數主鍵 id，避免和匯出時 tbiaID→id 衝突）──
with engine.connect() as conn:
    _rows = conn.execute(text("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'records'
    """)).fetchall()
int_cols = {r[0] for r in _rows if r[1] in ('integer', 'bigint', 'smallint')}
dt_cols = {r[0] for r in _rows if r[1].startswith('timestamp')}
export_cols = [c for c in records_cols if c != 'id']

q = lambda cols, p='': ', '.join(f'{p}"{c}"' for c in cols)
NOT_SKIP = 'NOT EXISTS (SELECT 1 FROM _rematch_skip s WHERE s."tbiaID" = r."tbiaID")'


def to_int64(s):
    return pd.to_numeric(s, errors='coerce').round().astype('Int64')


def run_sql(sql, params=None):
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(sql, params)
        raw.commit()
        return cur.rowcount
    finally:
        raw.close()


def copy_df(cur, df, table, cols):
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({q(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)


def join_cond(cols):
    return ' AND '.join(f'COALESCE(r."{c}", \'\') = t."{c}"' for c in cols)


def holder_ranges(cur, rh, size):
    cur.execute('''
        SELECT "tbiaID" FROM (
            SELECT "tbiaID", row_number() OVER (ORDER BY "tbiaID") AS rn
            FROM records WHERE "rightsHolder" = %s
        ) s WHERE mod(rn, %s) = 0 ORDER BY "tbiaID"
    ''', (rh, size))
    bounds = [r[0] for r in cur.fetchall()]
    return list(zip([''] + bounds, bounds + [None]))     # (lo, hi]，最後一段 hi=None


def range_cond(hi, alias='r'):
    return f'{alias}."tbiaID" > %(lo)s' + (f' AND {alias}."tbiaID" <= %(hi)s' if hi else '')


# ── 0. 工作表 ──
def setup_tables():
    name_defs = ', '.join(f'"{c}" text NOT NULL DEFAULT \'\'' for c in NAME_COLS)
    use_defs = ', '.join(f'"use_{c}" boolean' for c in NAME_COLS)
    stage_defs = ', '.join(f'"{c}" varchar(20)' for c in stage_cols)
    run_sql(f'''
        DROP TABLE IF EXISTS _rematch_names, _rematch_skip, _rematch_deleted, _rematch_holders;
        CREATE UNLOGGED TABLE _rematch_deleted (taxon_id varchar PRIMARY KEY);
        CREATE UNLOGGED TABLE _rematch_skip ("tbiaID" varchar PRIMARY KEY);
        CREATE UNLOGGED TABLE _rematch_holders (rights_holder text PRIMARY KEY, cfg_id int, {use_defs});
        CREATE UNLOGGED TABLE _rematch_names (
            cfg_id int, {name_defs},
            "taxonID" varchar(10), match_higher_taxon boolean, match_higher_partial boolean,
            match_stage int, {stage_defs}, is_matched boolean
        );
    ''')

    h_df = pd.DataFrame([{'rights_holder': rh, 'cfg_id': cfg_of(cols),
                          **{f'use_{c}': c in cols for c in NAME_COLS}} for rh, _, _, cols in holders])
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        copy_df(cur, h_df, '_rematch_holders', list(h_df.columns))
        copy_df(cur, pd.DataFrame({'taxon_id': sorted({str(x) for x in deleted_taxon_ids})}),
                '_rematch_deleted', ['taxon_id'])
        not_deleted = 'NOT EXISTS (SELECT 1 FROM _rematch_deleted d WHERE d.taxon_id = r."taxonID")'
        # (A) 來源已提供 taxonID（match_stage = 0）且未被刪除 → 不重新比對
        cur.execute(f'''
            INSERT INTO _rematch_skip ("tbiaID")
            SELECT m."tbiaID" FROM match_log m
            JOIN records r ON r."tbiaID" = m."tbiaID"
            WHERE m.match_stage = 0 AND r."taxonID" IS NOT NULL AND r."rightsHolder" = ANY(%s)
              AND {not_deleted}
            ON CONFLICT DO NOTHING
        ''', (holder_names,))
        skipped = cur.rowcount
        # (B) 會提供 taxonID 的單位、沒有 match_log 可判斷來源 → 保守起見不重新比對
        cur.execute(f'''
            INSERT INTO _rematch_skip ("tbiaID")
            SELECT r."tbiaID" FROM records r
            WHERE r."rightsHolder" = ANY(%s) AND r."taxonID" IS NOT NULL AND {not_deleted}
              AND NOT EXISTS (SELECT 1 FROM match_log m WHERE m."tbiaID" = r."tbiaID")
            ON CONFLICT DO NOTHING
        ''', ([h for h in holder_names if h in SRC_TAXON_HOLDERS],))
        skipped_no_ml = cur.rowcount
        raw.commit()
    finally:
        raw.close()
    print(f'來源提供 taxonID、不重新比對: {skipped} 筆')
    print(f'無 match_log、來源可能提供 taxonID、不重新比對: {skipped_no_ml} 筆')


# ── 1. 不重複學名組合（單次掃描）+ 比對 ──
def get_all_names():
    cache = f'{CACHE_DIR}/names_all.pkl'
    if os.path.exists(cache):
        print(f'使用快取學名組合: {cache}')
        return pd.read_pickle(cache)
    # 各單位沒用到的欄位一律投影成 ''，避免無關欄位造成組合膨脹
    sel = ', '.join(f'CASE WHEN h."use_{c}" THEN COALESCE(r."{c}", \'\') ELSE \'\' END AS "{c}"'
                    for c in NAME_COLS)
    print('取出不重複學名組合（掃描 records 一次）...')
    df = pd.read_sql(text(f'''
        SELECT DISTINCT h.cfg_id, {sel}
        FROM records r JOIN _rematch_holders h ON h.rights_holder = r."rightsHolder"
        WHERE {NOT_SKIP}
    '''), engine)
    df.to_pickle(cache)
    print(f'不重複學名組合: {len(df)}（' +
          '、'.join(f'cfg{k}: {v}' for k, v in df.cfg_id.value_counts().sort_index().items()) + '）')
    return df


def match_names(cols, names):
    names = names[cols].reset_index(drop=True)
    names['sci_index'] = names.index
    res = matching_flow_new_optimized(names.copy(), max_workers=MATCH_WORKERS)
    # 用 sci_index 接回原始學名（不依賴比對流程回傳的學名欄位）
    return names.merge(res[match_cols], on='sci_index', how='left').drop(columns='sci_index')


def get_match_results(cfg_id, names):
    cols = cfg_cols[cfg_id]
    cache = f'{CACHE_DIR}/cfg_{cfg_id}.pkl'
    partial = cache + '.partial'
    if os.path.exists(cache):
        return pd.read_pickle(cache)

    names = names[cols].drop_duplicates()
    results = []
    if os.path.exists(partial):
        prev = pd.read_pickle(partial)
        results.append(prev)
        names = names.merge(prev[cols], on=cols, how='left', indicator=True)
        names = names[names['_merge'] == 'left_only'].drop(columns='_merge')

    label = ','.join(h[1] for h in holders if cfg_of(h[3]) == cfg_id)
    with tqdm(total=len(names), unit='組', desc=f'比對 cfg{cfg_id} ({label})') as pbar:
        for i in range(0, len(names), NAME_CHUNK):
            chunk = match_names(cols, names.iloc[i:i + NAME_CHUNK])
            results.append(chunk)
            pd.concat(results, ignore_index=True).to_pickle(partial)
            pbar.update(len(chunk))

    df = (pd.concat(results, ignore_index=True).drop_duplicates(subset=cols)
          if results else pd.DataFrame(columns=cols + match_out_cols))
    df.to_pickle(cache)
    if os.path.exists(partial):
        os.remove(partial)
    return df


# ── 2. 寫入 _rematch_names ──
def load_names(cfg_id, df):
    if df.empty:
        return
    cols = cfg_cols[cfg_id]
    df = df.copy()
    df['taxonID'] = df['taxonID'].replace({'': None})      # 同原流程 to_none_dict：未對到 = NULL
    df = create_match_log_df(df, now)                      # 轉成 match_log 格式（is_matched、stage 文字）
    df['match_stage'] = to_int64(df['match_stage'])        # apply 回傳 int+None 時 pandas 會轉成 float（7.0）
    df['cfg_id'] = cfg_id
    for c in NAME_COLS:
        if c not in cols:
            df[c] = ''
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        copy_df(cur, df, '_rematch_names', ['cfg_id'] + NAME_COLS + ml_value_cols)
        raw.commit()
    finally:
        raw.close()


# ── 3. 分批更新 records + match_log ──
def update_holder(rh, group, cols):
    jc = join_cond(cols)
    ml_set = ', '.join(f'"{c}" = t."{c}"' for c in ml_value_cols)
    counts = dict(taxon=0, ml_update=0, ml_insert=0)

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        for lo, hi in tqdm(holder_ranges(cur, rh, BATCH), desc=f'更新 {group}', unit='批'):
            rng = range_cond(hi)
            p = {'rh': rh, 'lo': lo, 'hi': hi, 'cfg': cfg_of(cols), 'now': now}

            # (a) 比對結果有變動：taxonID、match_higher_taxon、modified
            cur.execute(f'''
                UPDATE records r SET "taxonID" = t."taxonID",
                    "match_higher_taxon" = t."match_higher_taxon", "modified" = %(now)s
                FROM _rematch_names t
                WHERE r."rightsHolder" = %(rh)s AND {rng} AND t.cfg_id = %(cfg)s AND {jc}
                  AND {NOT_SKIP}
                  AND ({q(rec_match_cols, 'r.')}) IS DISTINCT FROM ({q(rec_match_cols, 't.')})
            ''', p)
            counts['taxon'] += cur.rowcount

            # (b) match_log：有差異才更新
            cur.execute(f'''
                UPDATE match_log m SET {ml_set}, "modified" = %(now)s
                FROM records r JOIN _rematch_names t ON t.cfg_id = %(cfg)s AND {jc}
                WHERE m."tbiaID" = r."tbiaID" AND r."rightsHolder" = %(rh)s AND {rng} AND {NOT_SKIP}
                  AND ({q(ml_value_cols, 'm.')}) IS DISTINCT FROM ({q(ml_value_cols, 't.')})
            ''', p)
            counts['ml_update'] += cur.rowcount

            # (c) match_log 缺少的補上
            cur.execute(f'''
                INSERT INTO match_log ("tbiaID", "occurrenceID", "catalogNumber", "sourceScientificName",
                                       "group", "rights_holder", "created", "modified", {q(ml_value_cols)})
                SELECT r."tbiaID", r."occurrenceID", r."catalogNumber", r."sourceScientificName",
                       r."group", r."rightsHolder", %(now)s, %(now)s, {q(ml_value_cols, 't.')}
                FROM records r JOIN _rematch_names t ON t.cfg_id = %(cfg)s AND {jc}
                WHERE r."rightsHolder" = %(rh)s AND {rng} AND {NOT_SKIP}
                  AND NOT EXISTS (SELECT 1 FROM match_log m WHERE m."tbiaID" = r."tbiaID")
            ''', p)
            counts['ml_insert'] += cur.rowcount
            raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()
    tqdm.write(f'  {group}: taxonID 變動 {counts["taxon"]}、'
               f'match_log 更新 {counts["ml_update"]}、補新增 {counts["ml_insert"]}')


# ── 4. 輸出全部 records 成 Solr CSV ──
def sel_col(c, alias='r'):
    """timestamp 在 SQL 內轉字串（依 DB session 時區），避免 pandas 讀 timestamptz 時轉成 UTC 造成時間偏移
    （用於 sqlalchemy text()，冒號需跳脫成 \\:）"""
    if c in dt_cols:
        return f'to_char({alias}."{c}", \'YYYY-MM-DD HH24\\:MI\\:SS\') AS "{c}"'
    return f'{alias}."{c}"'


def normalize_db_df(df):
    for c in df.columns:
        if c in int_cols:
            df[c] = to_int64(df[c])
    # 原流程匯出到 Solr 的 created 一律等於當次更新時間（= modified；DB 的 created 不會被更新），這邊保持一致
    df['created'] = df['modified']
    return df


def export_with_taxon_checked(df, output_path, max_retries=3):
    """同 export_records_with_taxon（合併 taxa 欄位後輸出），但 taxa 缺漏時會重試，
    重試後仍缺的 taxonID 記錄到 log，避免 Solr 暫時錯誤造成匯出資料缺少 taxa 欄位"""
    ids = set(df['taxonID'].dropna())
    taxon_df, missing = pd.DataFrame(), ids
    for attempt in range(1, max_retries + 1):
        taxon_df = fetch_taxon_df(list(ids))
        missing = ids - (set(taxon_df['taxonID']) if len(taxon_df) else set())
        if not missing:
            break
        time.sleep(2 * attempt)
    if missing:
        with open(f'{CACHE_DIR}/missing_taxa.log', 'a') as f:
            f.write(f'{os.path.basename(output_path)}\t{",".join(sorted(missing))}\n')

    export_df = df.rename(columns={'tbiaID': 'id'})
    if len(taxon_df):
        export_df = export_df.merge(taxon_df, on='taxonID', how='left')
        if len(export_df) != len(df):
            raise RuntimeError(f'taxa merge 筆數不符: {len(df)} -> {len(export_df)} ({output_path})')
    export_df = export_df.rename(columns={'originalVernacularName': 'originalScientificName'})
    export_df.to_csv(output_path, index=None)


def export_task(args):
    n, rh, lo, hi = args
    try:
        engine.dispose(close=False)       # fork 後不共用父行程的連線
    except TypeError:
        engine.dispose()
    sql = text(f'SELECT {", ".join(sel_col(c) for c in export_cols)} FROM records r '
               'WHERE r."rightsHolder" = :rh AND '
               + range_cond(hi).replace('%(lo)s', ':lo').replace('%(hi)s', ':hi')
               + ' ORDER BY r."tbiaID"')
    df = pd.read_sql(sql, engine, params={'rh': rh, 'lo': lo, 'hi': hi})
    if df.empty:
        return 0
    export_with_taxon_checked(normalize_db_df(df), f'{EXPORT_DIR}/rematch_{n}.csv')
    return len(df)


def export_all():
    for f in glob.glob(f'{EXPORT_DIR}/rematch_*.csv') + glob.glob(f'{CACHE_DIR}/missing_taxa.log'):
        os.remove(f)                                         # 清掉上次中斷留下的檔案
    tasks = []
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        for rh, _, _, _ in update_holders:
            tasks += [(rh, lo, hi) for lo, hi in holder_ranges(cur, rh, EXPORT_CHUNK)]
        cur.execute('SELECT count(*) FROM records WHERE "rightsHolder" = ANY(%s)', (update_names,))
        total = cur.fetchone()[0]
    finally:
        raw.close()
    tasks = [(i, *t) for i, t in enumerate(tasks, 1)]

    with ProcessPoolExecutor(max_workers=EXPORT_WORKERS) as ex, \
            tqdm(total=total, unit='筆', desc='輸出 Solr CSV') as pbar:
        for fut in as_completed([ex.submit(export_task, t) for t in tasks]):
            pbar.update(fut.result())


# ── 5. 重建 match_log zip ──
def rebuild_match_log_zip(rh, group, info_id):
    out_cols = ['occurrenceID', 'catalogNumber', 'tbiaID', 'sourceScientificName', 'taxonID',
                'match_higher_taxon', 'match_higher_partial', 'match_stage'] + stage_cols + \
               ['group', 'rights_holder', 'created', 'modified', 'is_matched']
    csv_path = f'/portal/media/match_log/{group}_{info_id}_rematch.csv'
    sel = ', '.join(f'to_char(m."{c}", \'YYYY-MM-DD HH24\\:MI\\:SS\') AS "{c}"'
                    if c in ('created', 'modified') else f'm."{c}"' for c in out_cols)
    sql = text(f'SELECT {sel} FROM match_log m WHERE m.rights_holder = :rh')

    first = True
    with engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql(sql, conn, params={'rh': rh}, chunksize=200000):
            chunk['match_stage'] = to_int64(chunk['match_stage'])
            chunk.to_csv(csv_path, mode='w' if first else 'a', header=first, index=False)
            first = False
    if first:
        return
    zip_path = f'/portal/media/match_log/{group}_{info_id}_match_log.zip'
    if os.path.exists(zip_path):
        os.remove(zip_path)
    zip_match_log(group=group, info_id=info_id)


def main():
    os.makedirs(CACHE_DIR, exist_ok=True)
    setup_tables()

    # 1 + 2. 取組合 → 各組比對 → 寫入 _rematch_names
    all_names = get_all_names()
    for cfg_id in cfg_cols:
        df = get_match_results(cfg_id, all_names[all_names.cfg_id == cfg_id])
        load_names(cfg_id, df)
    run_sql(f'CREATE INDEX ON _rematch_names (cfg_id, "{SS}", "{SV}"); ANALYZE _rematch_names;')

    # 3. 更新 records + match_log
    for rh, group, info_id, cols in update_holders:
        update_holder(rh, group, cols)

    # 4. 全部輸出 Solr CSV
    export_all()

    # 5. match_log zip
    for rh, group, info_id, cols in tqdm(update_holders, desc='重建 match_log zip'):
        rebuild_match_log_zip(rh, group, info_id)

    # 收尾
    run_sql('DROP TABLE IF EXISTS _rematch_names, _rematch_skip, _rematch_deleted, _rematch_holders')
    for f in glob.glob(f'{CACHE_DIR}/*.pkl'):
        os.remove(f)
    if os.path.exists(f'{CACHE_DIR}/missing_taxa.log'):
        print(f'⚠️ 有 taxonID 在 taxa core 查不到，清單見 {CACHE_DIR}/missing_taxa.log')
    print(f'done! 本次 modified = {now_str}')
    print('⚠️ Solr 匯入請用 ACTION=import，不要執行 delete（沒變動的資料 modified 沒有更新）')
    print('建議執行 VACUUM (ANALYZE) records, match_log;')


if __name__ == '__main__':
    main()