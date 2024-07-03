

import subprocess
import pandas as pd
import math
import json


# TODO 這邊要修改成每次的deleted資料
df = pd.read_csv('deleted_2024-06-20.csv')

total_page = math.ceil(len(df) / 100)
limit = 100

df = df.drop(columns=['id'])
df['deleted'] = df['deleted'].apply(lambda x: x.split('.')[0])
df = df.rename(columns={'tbiaID': 'id'})

results = df.to_dict('records')

print('total_page', total_page)
for p in range(0,total_page):
    print(p)
    deleting_rs = results[p*limit:(p+1)*limit]
    deleting_ids = [d['id'] for d in deleting_rs]
    deleting_str = ' OR '.join(deleting_ids)
    commands = f''' curl http://solr:8983/solr/tbia_records/update/?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>id:({deleting_str})</query></delete>'; ''' 
    process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    a = process.communicate()
    # 移到 deleted_records
    # deleting_df = pd.DataFrame(deleting_rs)
    # deleting_df = df[]
    # deleting_df['deleted'] = deleting_df['deleted'].apply(lambda x: x.split('.')[0])
    # deleting_dict = deleting_df.to_dict('records')
    deleting_dict = json.dumps(deleting_rs)
    commands = f''' curl http://solr:8983/solr/deleted_records/update/?commit=true -H "Content-Type: application/json" --data-binary '{deleting_dict}'; ''' 
    process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    a = process.communicate()