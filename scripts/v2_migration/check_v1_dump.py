"""
檢查 dump_v1_core 匯出的 CSV 與原本 v1 CSV 是否相同（2.6 轉檔前跑；只讀）

拿 /solr/csvs/export 的 rematch_*.csv（最新一次寫進 v1 core 的版本）當基準，
用 id 到 dump 目錄找同一筆，逐欄比較。比較前的正規化：
  空字串 / NaN / 欄位不存在視為同樣的空值；數值以 float 比；日期去掉毫秒後比字串。
輸出每個欄位不同的筆數與例子，全部相同印 [PASS]。

用法（測試站 ~/tbia-datahub-v2，dump 完成後）：
  docker compose -f docker-compose.v2.yml exec app-v2 python -u -m scripts.v2_migration.check_v1_dump \
    --dump /bucket/v2/v1_dump --files rematch_119.csv rematch_14.csv rematch_186.csv rematch_160.csv
"""
import argparse
import csv
import glob
import os
import re
import sys
from collections import Counter, defaultdict

csv.field_size_limit(sys.maxsize)
DT_RE = re.compile(r'^(\d{4}-\d\d-\d\d)[ T](\d\d:\d\d:\d\d)(?:\.\d+)?Z?$')
EMPTY = {'', 'nan', 'NaN', 'None'}


def norm(v):
    if v is None:
        return ''
    v = str(v).strip()
    if v in EMPTY:
        return ''
    m = DT_RE.match(v)
    if m:
        return f'{m.group(1)} {m.group(2)}'
    try:
        f = float(v)
        if f == f:   # 不是 NaN
            return repr(f)
    except ValueError:
        pass
    if v in ('true', 'True'):
        return 'True'
    if v in ('false', 'False'):
        return 'False'
    return v


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--src', default='/solr/csvs/export')
    p.add_argument('--dump', required=True)
    p.add_argument('--files', nargs='+', required=True, help='src 中當基準的 v1 CSV 檔名')
    a = p.parse_args(argv)

    base = {}
    for f in a.files:
        with open(os.path.join(a.src, f), newline='', encoding='utf-8') as fh:
            for row in csv.DictReader(fh):
                base[row['id']] = row
    print(f'[check] 基準 {len(a.files)} 檔、{len(base):,} 筆', flush=True)

    found = {}
    dump_files = sorted(glob.glob(os.path.join(a.dump, 'v1_dump_*.csv')))
    for i, f in enumerate(dump_files, 1):
        with open(f, newline='', encoding='utf-8') as fh:
            for row in csv.DictReader(fh):
                if row['id'] in base:
                    found[row['id']] = row
        if i % 100 == 0:
            print(f'[check] 掃描 {i} / {len(dump_files)} 檔，找到 {len(found):,} 筆', flush=True)

    missing = set(base) - set(found)
    diff = Counter()
    examples = defaultdict(list)
    only_dump_cols = Counter()
    for i, row in found.items():
        b = base[i]
        for c in set(b) | set(row):
            if c not in b:
                if norm(row.get(c)):
                    only_dump_cols[c] += 1
                continue
            if norm(b.get(c)) != norm(row.get(c)):
                diff[c] += 1
                if len(examples[c]) < 3:
                    examples[c].append((i, b.get(c), row.get(c)))

    print(f'[check] dump 找到 {len(found):,} / {len(base):,} 筆，找不到 {len(missing):,} 筆'
          + (f'（例：{sorted(missing)[:5]}）' if missing else ''))
    for c, n in diff.most_common():
        print(f'[check] 欄位 {c} 不同 {n:,} 筆，例（id, 原 CSV, dump）：{examples[c]}')
    if only_dump_cols:
        print(f'[check] 只在 dump 有值的欄位（原 CSV 沒有這欄）：{dict(only_dump_cols)}')
    ok = not missing and not diff
    print('[PASS] dump 與原 v1 CSV 相同' if ok else '[FAIL] 見上方差異')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
