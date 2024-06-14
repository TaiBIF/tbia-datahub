# 計算後台統計
# from manager.models import *
# import json
# import urllib.parse
# import pandas as pd
# from django.utils import timezone
import requests
import json
import psycopg2
from app import portal_db_settings

# TODO 每次更新的時候重寫這邊
year_month = ''

    
rights_holder_map = {
    'GBIF': 'gbif',
    '中央研究院生物多樣性中心植物標本資料庫': 'brmas',
    '台灣生物多樣性網絡 TBN': 'tbri',
    '國立臺灣博物館典藏': 'ntm',
    '林業試驗所昆蟲標本館': 'fact',
    '林業試驗所植物標本資料庫': 'taif',
    '河川環境資料庫': 'wra',
    '濕地環境資料庫': 'nps',
    '生態調查資料庫系統': 'forest',
    '臺灣國家公園生物多樣性資料庫': 'nps',
    '臺灣生物多樣性資訊機構 TaiBIF': 'brcas',
    '海洋保育資料倉儲系統': 'oca'
}

query = { "query": "*:*",
        "offset": 0,
        "limit": 0,
        # "filter": fq_list,
        }
# 查詢記錄
# if offset == 0:
query['facet'] = {}
query['facet']['stat_rightsHolder'] = {
    'type': 'terms',
    'field': 'rightsHolder',
    'mincount': 1,
    'limit': -1,
    'allBuckets': False,
    'numBuckets': False}
# if not fq_list:
#     query.pop('filter')


response = requests.post(f'http://solr:8983/solr/tbia_records/select', data=json.dumps(query), headers={'content-type': "application/json" })
response = response.json()
# 整理欄位
total = response['response']['numFound']
# data = response['response']['docs']
stat_rightsHolder = []
# if 'stat_rightsHolder' in response['facets'].keys():
stat_rightsHolder = response['facets']['stat_rightsHolder']['buckets']
stat_rightsHolder.append({'val': 'total', 'count': total})

for d in stat_rightsHolder:
    if d['val'] in rights_holder_map.keys():
        group = rights_holder_map[d['val']]
    else:
        group = 'total'
    query = """
                INSERT INTO deleted_records ("year_month", "count", "rights_holder", "group", "type")
                VALUES(%s, %s, %s, %s, 'data' );
                """.format(year_month, d['count'], d['val'], group)
    conn = psycopg2.connect(**portal_db_settings)
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        conn.commit()

    # DataStat.objects.create(
    #     year_month = year_month,
    #     count = d['count'],
    #     rights_holder= d['val'],
    #     group=group,
    #     type = 'data'
    # )


