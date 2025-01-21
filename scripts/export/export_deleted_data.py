import subprocess
import pandas as pd
from app import db
from sqlalchemy import text
import sqlalchemy as sa
import math
import glob
import json

# TODO 這邊要修改
deleted_time = '2024-10-25'

total_count = 0

limit = 10000
offset = 0
min_id = 1
has_more_data = True
while has_more_data:
    print(offset)
    with db.begin() as conn:
        qry = sa.text('''SELECT * FROM deleted_records WHERE
                       id > {} and deleted > '{}' order by id limit {} ; 
                      '''.format(min_id, deleted_time, limit))
        resultset = conn.execute(qry)
        results = resultset.mappings().all()
        df = pd.DataFrame(results)
        min_id = df.id.max()
        # 這邊要匯出給正式站
        df.to_csv('/bucket/tbia_deleted_{}_{}.csv'.format(deleted_time,offset), index=None)
        offset += limit
    if len(results) < limit:
        has_more_data = False




files = glob.glob("/bucket/tbia_deleted_{}_*.csv".format(deleted_time))

for f in files:
    print(f)
    df = pd.read_csv(f)
    total_page = math.ceil(len(df) / 100)
    limit = 100
    df = df.drop(columns=['id'])
    df['deleted'] = df['deleted'].apply(lambda x: x.split('.')[0])
    df = df.rename(columns={'tbiaID': 'id'})
    results = df.to_dict('records')
    print('total_page', total_page)
    for p in range(0,total_page):
        # print(p)
        deleting_rs = results[p*limit:(p+1)*limit]
        deleting_ids = [d['id'] for d in deleting_rs]
        deleting_str = ' OR '.join(deleting_ids)
        commands = f''' curl http://solr:8983/solr/tbia_records/update/?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>id:({deleting_str})</query></delete>'; ''' 
        process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        a = process.communicate()
        # 移到 deleted_records
        deleting_dict = json.dumps(deleting_rs)
        commands = f''' curl http://solr:8983/solr/deleted_records/update/?commit=true -H "Content-Type: application/json" --data-binary '{deleting_dict}'; ''' 
        process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        a = process.communicate()
