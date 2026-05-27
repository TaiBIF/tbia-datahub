import requests
import pandas as pd
from datetime import datetime

from solr_helper import SOLR_BASE, solr_reimport_from_csv


now = datetime.now().strftime('%Y%m%d')

params = {
    'q': '*:*',
    'q.op': 'OR',
    'rows': 0,
    'facet': 'true',
    'facet.limit': -1,
    'facet.pivot': 'locality,recordType',
}
response = requests.get(f'{SOLR_BASE}/tbia_records/select', params=params)
response.raise_for_status()
resp = response.json()

locality_list = resp['facet_counts']['facet_pivot']['locality,recordType']
rows = []
for l in locality_list:
    for ll in l.get('pivot') or []:
        rows.append({'locality': l.get('value'), 'record_type': ll.get('value')})

locality = pd.DataFrame(rows)
updating_csv = f'/bucket/tbia_locality_{now}.csv'
locality.to_csv(updating_csv)


# 刪除 Solr locality core 並從 CSV 重匯
solr_reimport_from_csv('locality', updating_csv)