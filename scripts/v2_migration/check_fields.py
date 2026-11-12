"""
B2 / 2.6 前檢查：v1 有、但 v2 匯出會丟掉的欄位（一次性工具，上線後與本目錄一起刪除）

  docker compose -f docker-compose.v2.yml exec -T app-v2 python -m scripts.v2_migration.check_fields

1. 線上 tbia_records 中由 schemaless 自動建立的欄位（repo 的 v1 managed-schema 沒有定義）
2. /solr/csvs/export 所有 CSV 的欄位，經 to_v2 改名後不在 v2 schema（= 2.6 會被丟掉）的欄位
兩者都是空的，才可以放心跑 2.6。
"""
import argparse
import glob
import os
import re
from collections import Counter
from pathlib import Path

import requests

from scripts.utils.export import RECORDS_DROP, RECORDS_RENAME, SOLR_URL, TAXA_RENAME, v2_schema_fields

V1_SCHEMA = Path(__file__).resolve().parents[2] / 'solr-workspace/conf-tbia/tbia_records_v1/managed-schema'

# to_v2 刻意不輸出的欄位（v1 CSV 有、v2 不需要），不算遺漏
INTENDED_DROP = set(RECORDS_DROP) | {'id'}


def v1_defined():
    text = V1_SCHEMA.read_text(encoding='utf-8')
    return set(re.findall(r'<field name="([^"]+)"', text))


def live_fields(core):
    r = requests.get(f'{SOLR_URL}/{core}/schema/fields', params={'wt': 'json'})
    r.raise_for_status()
    return {f['name']: f for f in r.json()['fields']}


def csv_headers(src):
    cnt = Counter()
    files = sorted(glob.glob(os.path.join(src, '*.csv')))
    for f in files:
        with open(f, encoding='utf-8') as fh:
            for c in fh.readline().rstrip('\r\n').split(','):
                cnt[c.strip('"')] += 1
    return cnt, len(files)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--core', default='tbia_records')
    p.add_argument('--src', default='/solr/csvs/export')
    a = p.parse_args()

    v2 = v2_schema_fields()
    rename = {**RECORDS_RENAME, **TAXA_RENAME}

    print(f'[1] {a.core} 由 schemaless 自動建立的欄位（不在 repo v1 schema）')
    auto = {k: v for k, v in live_fields(a.core).items() if k not in v1_defined() and not k.startswith('_')}
    for k, v in sorted(auto.items()):
        v2_name = rename.get(k, k)
        status = '已在 v2' if v2_name in v2 else '!! v2 沒有'
        print(f'    {k:<35} {v.get("type"):<12} multiValued={v.get("multiValued", False)!s:<5} → {v2_name}（{status}）')
    if not auto:
        print('    （無）')

    cnt, n = csv_headers(a.src)
    print(f'\n[2] {a.src} 共 {n} 個 CSV；改名後不在 v2 schema 的欄位（2.6 會丟掉）')
    lost = {c: k for c, k in cnt.items()
            if c not in INTENDED_DROP and rename.get(c, c) not in v2 and c != ''}
    for c, k in sorted(lost.items()):
        print(f'    !! {c:<35} 出現在 {k} 個檔案')
    if not lost:
        print('    （無）')

    print('\n結論：', '可以跑 2.6' if not lost and all(rename.get(k, k) in v2 for k in auto) else '有欄位會遺漏，先補 v2 schema 或確認可丟')


if __name__ == '__main__':
    main()
