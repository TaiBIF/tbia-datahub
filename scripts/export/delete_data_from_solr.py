import subprocess
import pandas as pd
from app import db
from sqlalchemy import text
import sqlalchemy as sa


deleted_time = ''

with db.begin() as conn:
    qry = sa.text('''SELECT * FROM deleted_records WHERE deleted > '{}'; '''.format(deleted_time))
    resultset = conn.execute(qry)
    results = resultset.mappings().all()


import math
import json

total_page = math.ceil(len(results) / 1000)
limit = 1000

for p in range(0,total_page):
    print(p)
    deleting_rs = results[p*limit:(p+1)*1000]
    deleting_ids = [d['tbiaID'] for d in deleting_rs]
    deleting_str = ' OR '.join(deleting_ids)
    commands = f''' curl http://solr:8983/solr/tbia_records/update/?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>id:({deleting_str})</query></delete>'; ''' 
    process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    a = process.communicate()
    # 移到 deleted_records
    deleting_df = pd.DataFrame(deleting_rs)
    deleting_df['deleted'] = deleting_df['deleted'].dt.strftime('%Y-%m-%d %H:%M:%S')
    deleting_dict = deleting_df.to_dict('records')
    deleting_dict = json.dumps(deleting_dict)
    commands = f''' curl http://solr:8983/solr/tbia_records/update/?commit=true -H "Content-Type: application/json" --data-binary '{deleting_dict}'; ''' 
    process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    a = process.communicate()


