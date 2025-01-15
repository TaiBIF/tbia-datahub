import subprocess
import pandas as pd
from app import db
from sqlalchemy import text
import sqlalchemy as sa


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

# with db.begin() as conn:
#     qry = sa.text('''select * from deleted_records order by id desc limit 1;; ''')
#     resultset = conn.execute(qry)
#     results = resultset.mappings().all()

#  這邊要匯出給正式站

# df = pd.DataFrame(results)
# df.to_csv('deleted_{}.csv'.format(deleted_time), index=None)





import math
import json

total_page = math.ceil(len(results) / 100)
limit = 100

print('total_page', total_page)
for p in range(0,total_page):
    print(p)
    deleting_rs = results[p*limit:(p+1)*limit]
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
    commands = f''' curl http://solr:8983/solr/deleted_records/update/?commit=true -H "Content-Type: application/json" --data-binary '{deleting_dict}'; ''' 
    process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    a = process.communicate()





# curl http://127.0.0.1:8983/solr/tbia_records/update/?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>(group:gbif)AND(datasetName:EOD – eBird Observation Dataset) </query></delete>'; 
# curl http://127.0.0.1:8983/solr/tbia_records/update/?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>(rightsHolder:濕地環境資料庫)AND(created: [* TO 2023-10-18T00:00:00Z]) </query></delete>'; 