# python -m scripts.post_update.dev.locality [--dry-run]
import argparse

import requests
import pandas as pd
from datetime import datetime

from scripts.post_update.solr_helper import SOLR_BASE, RECORDS_CORE, LOCALITY_CORE, solr_reimport_from_csv

parser = argparse.ArgumentParser(description='更新 locality core')
parser.add_argument('--dry-run', action='store_true', help='只輸出 CSV，不寫入 locality core')
args = parser.parse_args()

now = datetime.now().strftime('%Y%m%d')

params = {
    'q': '*:*',
    'q.op': 'OR',
    'rows': 0,
    'facet': 'true',
    'facet.limit': -1,
    'facet.pivot': 'locality,recordType',
}
response = requests.get(f'{SOLR_BASE}/{RECORDS_CORE}/select', params=params)
response.raise_for_status()
resp = response.json()

locality_list = resp['facet_counts']['facet_pivot']['locality,recordType']
rows = []
for l in locality_list:
    for ll in l.get('pivot') or []:
        rows.append({'locality': l.get('value'), 'record_type': ll.get('value')})

locality = pd.DataFrame(rows)
updating_csv = f'/bucket/tbia_locality_{now}{"_dryrun" if args.dry_run else ""}.csv'
locality.to_csv(updating_csv)
print(f'已匯出 {updating_csv}（{len(locality)} 筆，來源 {RECORDS_CORE}）')

if args.dry_run:
    raise SystemExit('--dry-run：未寫入 locality core')

# 刪除 Solr locality core 並從 CSV 重匯
solr_reimport_from_csv(LOCALITY_CORE, updating_csv)