import requests
import pandas as pd
from datetime import datetime

now = datetime.now().strftime('%Y%m%d')


url = "http://solr:8983/solr/tbia_records/select?facet.pivot=locality,recordType&facet.limit=-1&facet=true&q.op=OR&q=*%3A*&rows=0"
response = requests.get(url)
if response.status_code == 200:
    resp = response.json()
    locality_list = resp['facet_counts']['facet_pivot']['locality,recordType'] # 264570
    rows = []
    for l in locality_list:
        record_type = []
        for ll in l.get('pivot'):
            rows.append({'locality': l.get('value'), 'record_type': ll.get('value')})
    locality = pd.DataFrame(rows)
    updating_csv = '/bucket/tbia_locality_{}.csv'.format(now)
    locality.to_csv(updating_csv)


# 把刪除 & 重新匯入也寫進這段程式碼

import subprocess

commands = f''' curl http://solr:8983/solr/locality/update/?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>*:*</query></delete>'; ''' 
process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
a = process.communicate()


commands = f''' curl http://solr:8983/solr/locality/update/?commit=true -H "Content-Type: text/csv" --data-binary @{updating_csv}; ''' 
process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
a = process.communicate()