"""
v1 CSV → v2 CSV（一次性工具，上線後刪除這個目錄）

讀 update 產出的 v1 CSV（/solr/csvs/export，即線上 tbia_records 的完整內容），用 export.to_v2 轉成 v2 欄位，
另寫一份到 --out，不改動原檔。用於 2.5 部分來源驗證與 2.6 全量匯出；可選擇匯入 core 並與 v1 抽樣比對。

使用範例（測試站 ~/tbia-datahub-v2，背景執行）：
  docker compose -f docker-compose.v2.yml exec app-v2 python -m scripts.v2_migration.convert_v1_csv --list

  docker compose -f docker-compose.v2.yml exec -d app-v2 sh -c \
    'python -u -m scripts.v2_migration.convert_v1_csv --group nmns --taxa-version 20261030 --overwrite --post --replace --compare 200 \
     > /tmp/export/nmns.log 2>&1'

  # 同 group 多個資料庫：--pattern 指定檔名（nps_0_* 國家公園、nps_1_* 濕地），--rights-holder 讓比對只看該資料庫
  ... --group nps --pattern 'nps_1_*.csv' --rights-holder 濕地環境資料庫 ...
"""
import argparse
import glob
import json
import os
import re
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests

from scripts.utils.export import (RECORDS_RENAME, TAXA_RENAME, SOLR_URL, TAXA_SELECT_URL,
                                  load_derived_sources, set_taxa_version, source_database_map, to_v2)

PROTECTED_CORES = ('tbia_records', 'tbia_records_build', 'tbia_records_v1')

# ---------------------------------------------------------------------------
# 轉檔
# ---------------------------------------------------------------------------

_name_id_cache = {}


def fetch_name_ids(taxon_ids, chunk_size=500):
    """v1 CSV 匯出時 drop 掉了 taxon_name_id，只補這個欄位（其餘 taxa 欄位沿用 CSV 內的值）。"""
    todo = [t for t in set(taxon_ids) if t and t not in _name_id_cache]
    for i in range(0, len(todo), chunk_size):
        chunk = todo[i:i + chunk_size]
        r = requests.post(TAXA_SELECT_URL, headers={'content-type': 'application/json'},
                          data=json.dumps({'query': '{!terms f=id}' + ','.join(chunk),
                                           'fields': 'id,taxon_name_id', 'limit': len(chunk)}))
        if r.status_code != 200:
            raise RuntimeError(f'taxa solr error {r.status_code}')
        for d in r.json()['response']['docs']:
            _name_id_cache[d['id']] = d.get('taxon_name_id')
        for t in chunk:
            _name_id_cache.setdefault(t, None)
    ids = [t for t in set(taxon_ids) if t]
    return pd.DataFrame({'taxonID': ids, 'taxon_name_id': [_name_id_cache[t] for t in ids]})


def convert_file(src, out_dir):
    # 全部以字串讀入、只把空字串視為空值，'NaN' 等字串交給 to_v2 處理，避免 pandas 改動原值
    df = pd.read_csv(src, dtype=str, keep_default_na=False, na_values=[''])
    taxon_df = fetch_name_ids(df['taxonID'].dropna().tolist()) if 'taxonID' in df.columns else None
    dst = os.path.join(out_dir, os.path.basename(src))
    to_v2(df, taxon_df).to_csv(dst, index=None)
    return dst, len(df)


def convert(files, out_dir, workers):
    done, total = [], 0
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(convert_file, f, out_dir): f for f in files}
        for i, fut in enumerate(as_completed(futs), 1):
            dst, n = fut.result()     # 任一檔失敗就中止，避免匯出不完整的資料
            done.append(dst)
            total += n
            if i % 20 == 0 or i == len(files):
                print(f'[convert] {i} / {len(files)} 檔，累計 {total:,} 筆')
    return sorted(done), total


# ---------------------------------------------------------------------------
# 匯入 core
# ---------------------------------------------------------------------------

def solr_count(core, q):
    r = requests.get(f'{SOLR_URL}/{core}/select', params={'q': q, 'rows': 0, 'wt': 'json'})
    r.raise_for_status()
    return r.json()['response']['numFound']


def post_files(files, core, replace_q=None):
    if replace_q:
        print(f'[post] 刪除 {core} 中 {replace_q}：{solr_count(core, replace_q):,} 筆')
        requests.post(f'{SOLR_URL}/{core}/update', params={'commit': 'true'},
                      json={'delete': {'query': replace_q}}).raise_for_status()
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


# ---------------------------------------------------------------------------
# 與 v1 比對
# ---------------------------------------------------------------------------

# v1 Solr 欄位 → v2 欄位
V1_TO_V2 = {**{k: v for k, v in RECORDS_RENAME.items() if k not in ('tbiaID', 'originalVernacularName')},
            **{k: v for k, v in TAXA_RENAME.items() if k != 'taxon_name_id'}}
V1_SKIP = {'organismQuantity', 'originalVernacularName'}   # v1 原始數量字串 v2 不存；originalVernacularName v1 為空
# 規則本來就會不同的欄位：只計數，不列為異常
EXPECTED_DIFF = {'typeStatus', 'verbatimLatitude', 'verbatimLongitude', 'dataGeneralizations',
                 'scientificNameID', 'sourceDatabase', 'coordinatePrecision', 'coordinateUncertaintyInMeters',
                 # 2.3：v1 標了模糊化但座標沒動的紀錄，v2 清空 raw*（export.RAW_COLS）
                 'rawDecimalLatitude', 'rawDecimalLongitude', 'rawVerbatimLatitude', 'rawVerbatimLongitude',
                 'rawCounty', 'rawMunicipality', 'raw_location_rpt'}


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
    return s.lower() if s.lower() in ('true', 'false') else s


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
    r = requests.post(f'{SOLR_URL}/{v1_core}/select',
                      json={'query': '{!terms f=id}' + ','.join(new_docs), 'limit': len(new_docs)})
    r.raise_for_status()
    old_docs = {d['id']: d for d in r.json()['response']['docs']}
    print(f'[compare] 抽樣 {len(new_docs)} 筆，v1 找到 {len(old_docs)} 筆')

    diff, examples = Counter(), {}
    for i, new in new_docs.items():
        old = old_docs.get(i)
        if old is None:
            continue
        for k1, val in old.items():
            if k1.startswith('_') or k1 in V1_SKIP:
                continue
            k2 = V1_TO_V2.get(k1, k1)
            if _norm(val) != _norm(new.get(k2)):
                diff[k2] += 1
                examples.setdefault(k2, (i, val, new.get(k2)))
        for k2, val in new.items():
            if not k2.startswith('_') and k2 not in V1_TO_V2.values() and k2 not in old and val not in (None, ''):
                diff[f'{k2}（v2 新增）'] += 1

    if not diff:
        print('[compare] 所有欄位一致')
    expected = lambda k: k in EXPECTED_DIFF or '新增' in k
    for k in sorted(diff, key=lambda x: (expected(x), x)):
        ex = examples.get(k)
        ex_s = f'  例 {ex[0]}: {ex[1]!r} → {ex[2]!r}' if ex else ''
        print(f'  [{"預期" if expected(k) else "異常"}] {k}: {diff[k]} 筆{ex_s}')


# ---------------------------------------------------------------------------

def list_groups(src):
    stat = {}
    for f in glob.glob(os.path.join(src, '*.csv')):
        g = os.path.basename(f).split('_')[0]
        n, size = stat.get(g, (0, 0))
        stat[g] = (n + 1, size + os.path.getsize(f))
    for g, (n, size) in sorted(stat.items(), key=lambda x: x[1][1]):
        print(f'{g:<12} {n:>6} 檔  {size / 1024 ** 3:8.2f} GB')


def main(argv=None):
    p = argparse.ArgumentParser(description='v1 CSV → v2 CSV（可匯入 core 並與 v1 比對）')
    p.add_argument('--src', default='/solr/csvs/export', help='v1 CSV 目錄（只讀）')
    p.add_argument('--out', default='/tmp/export', help='v2 CSV 輸出目錄')
    p.add_argument('--list', action='store_true', help='列出 src 中各 group 的檔案數與大小')
    p.add_argument('--group')
    p.add_argument('--pattern', help="檔名樣式，預設 '<group>_*.csv'")
    p.add_argument('--rights-holder', help='比對時只看這個資料庫（舊名稱）')
    p.add_argument('--taxa-version', help='taxa 來源檔名日期 YYYYMMDD（nameAccordingTo，D8）；未給則用環境變數 TAXA_VERSION')
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--overwrite', action='store_true', help='先刪除 out 中同樣式的舊檔')
    p.add_argument('--post', action='store_true', help='轉完匯入 --core')
    p.add_argument('--core', default='tbia_records_new')
    p.add_argument('--replace', action='store_true', help='匯入前刪除 core 中該單位的資料')
    p.add_argument('--compare', type=int, default=0, metavar='N', help='與 v1 core 比筆數並抽 N 筆比欄位')
    p.add_argument('--v1-core', default='tbia_records')
    a = p.parse_args(argv)

    if a.list:
        list_groups(a.src)
        return
    if not a.group:
        p.error('需要 --group（或 --list）')
    if a.core in PROTECTED_CORES and (a.post or a.replace):
        sys.exit(f'拒絕寫入 {a.core}：開發期只寫 tbia_records_new，正式灌入走 import_and_swap_data.sh')
    if a.pattern and a.replace and not a.rights_holder:
        sys.exit('用 --pattern 只轉部分檔案時，--replace 需搭配 --rights-holder，否則會刪掉整個 group')
    if os.path.abspath(a.out) == os.path.abspath(a.src):
        sys.exit('--out 不可與 --src 相同（會覆蓋 v1 CSV）')

    pattern = a.pattern or f'{a.group}_*.csv'
    files = sorted(glob.glob(os.path.join(a.src, pattern)))
    if not files:
        sys.exit(f'{a.src} 找不到 {pattern}')
    os.makedirs(a.out, exist_ok=True)
    old = glob.glob(os.path.join(a.out, pattern))
    if old:
        if not a.overwrite:
            sys.exit(f'{a.out} 已有 {len(old)} 個 {pattern}，加 --overwrite 先刪除')
        for f in old:
            os.remove(f)

    if a.taxa_version:
        set_taxa_version(a.taxa_version)
    load_derived_sources()   # 主程序先載入，子程序 fork 後沿用

    t0 = time.time()
    print(f'[convert] {len(files)} 個檔案（{a.src}/{pattern}）→ {a.out}，workers={a.workers}')
    out_files, total = convert(files, a.out, a.workers)
    print(f'[convert] 完成：{total:,} 筆、{len(out_files)} 個檔案，{time.time() - t0:,.0f} 秒')

    v1_q = f'group:"{a.group}"' + (f' AND rightsHolder:"{a.rights_holder}"' if a.rights_holder else '')
    new_rh = source_database_map().get(a.rights_holder, a.rights_holder) if a.rights_holder else None
    v2_q = f'group:"{a.group}"' + (f' AND sourceDatabase:"{new_rh}"' if new_rh else '')

    if a.post:
        t1 = time.time()
        post_files(out_files, a.core, replace_q=v2_q if a.replace else None)
        print(f'[post] {time.time() - t1:,.0f} 秒')
    if a.compare:
        compare_with_v1(a.v1_core, a.core, v1_q, v2_q, a.compare)


if __name__ == '__main__':
    main()