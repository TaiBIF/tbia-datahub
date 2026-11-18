"""檢查 dataGeneralizations 新文字（3.3）：讀 v1 CSV 的某個單位，用 export.to_v2 轉換後統計文字。
只在記憶體轉換、不寫檔、不匯入 Solr。
用法（在 datahub 根目錄、與 convert_v1_csv.py 相同的環境）：
  python -m scripts.v2_migration.check_dg_text --group ardswc
  python -m scripts.v2_migration.check_dg_text --group taif --limit 300000
"""
import argparse
import glob
import os

import pandas as pd

from scripts.utils.export import DG_PREFIX, to_v2

p = argparse.ArgumentParser()
p.add_argument('--group', required=True)
p.add_argument('--src', default='/solr/csvs/export')
p.add_argument('--limit', type=int, default=200000, help='最多讀幾筆（預設 20 萬）')
a = p.parse_args()

frames, n = [], 0
for path in sorted(glob.glob(os.path.join(a.src, '*.csv'))):
    head = pd.read_csv(path, dtype=str, nrows=0)
    if 'group' not in head.columns:
        continue
    for chunk in pd.read_csv(path, dtype=str, chunksize=50000):
        chunk = chunk[chunk['group'] == a.group]
        if len(chunk):
            frames.append(chunk)
            n += len(chunk)
        if n >= a.limit:
            break
    if n >= a.limit:
        break
if not frames:
    raise SystemExit(f'{a.src} 找不到 group={a.group} 的資料')

df = pd.concat(frames, ignore_index=True).head(a.limit)
v1_dg = df['dataGeneralizations'].fillna('（空）').value_counts() if 'dataGeneralizations' in df else None
out = to_v2(df, derive=False)
dg = out['dataGeneralizations'].fillna('（空）')

print(f'group={a.group}，讀取 {len(df):,} 筆')
if v1_dg is not None:
    print('v1 dataGeneralizations：', v1_dg.to_dict())
print('v2 dataGeneralizations：')
for text, cnt in dg.value_counts().items():
    print(f'  {cnt:>10,}  {text}')

bad = dg[(dg != '（空）') & ~dg.str.startswith(DG_PREFIX)]
print('不是新文字開頭的筆數：', len(bad), '（應為 0）')
raw = [c for c in ('rawDecimalLatitude', 'rawVerbatimLatitude') if c in out.columns]
if raw:
    has_raw = out[raw].notna().any(axis=1)
    print('有 raw 但沒有文字：', int((has_raw & (dg == '（空）')).sum()), '（應為 0）')
    print('有文字但沒有 raw：', int((~has_raw & (dg != '（空）')).sum()), '（應為 0）')
