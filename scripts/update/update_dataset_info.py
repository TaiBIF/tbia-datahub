# 計算資料集統計
import requests
import json
import psycopg2
from app import portal_db_settings, db_settings
import requests
# from conf.settings import SOLR_PREFIX
import json
from scripts.utils import taxon_group_map, rights_holder_map
import pandas as pd
from datetime import datetime
import numpy as np


bio_group_en = {
    "昆蟲": "Insects",
    "蜘蛛": "Spiders",
    "魚類": "Fishes",
    "爬蟲類": "Reptiles",
    "兩棲類": "Amphibians",
    "鳥類": "Birds",
    "哺乳類": "Mammals",
    "維管束植物": "Vascular Plants",
    "蕨類植物": "Ferns",
    "苔蘚植物": "Mosses",
    "藻類": "Algae",
    "病毒": "Viruses",
    "細菌": "Bacteria",
    "真菌": "Fungi",
}

# 先取得所有未棄用的資料集

conn = psycopg2.connect(**db_settings)
query = """select "tbiaDatasetID", "name", "rights_holder" from dataset WHERE deprecated = 'f';"""
with conn.cursor() as cursor:
    cursor.execute(query)
    res = cursor.fetchall()
    df = pd.DataFrame(res, columns=["tbiaDatasetID","datasetName","rights_holder"])



# occurrenceCount, datasetDateCoverage, resourceContacts


for i in df.index:
    if i % 100 == 0:
        print(i)
    row = df.iloc[i]
    query_list = []
    # 測試先用 rightsHolder + datasetName
    query_list += ['tbiaDatasetID:"{}"'.format(row.tbiaDatasetID)]
    # query_list += ['rightsHolder:"{}"'.format(row.rights_holder)]
    # query_list += ['datasetName:"{}"'.format(row.datasetName)]
    query = { "query": "*:*",
            "offset": 0,
            "limit": 0,
            "filter": query_list,
            }   
    response = requests.post('http://solr:8983/solr/tbia_records/select?stats=true&stats.field=standardDate&facet.field=resourceContacts&facet.limit=-1&facet.mincount=1&facet=true', data=json.dumps(query), headers={'content-type': "application/json" })
    resp = response.json()
    occurrenceCount = resp['response']['numFound']
    df.loc[i, 'occurrenceCount'] = occurrenceCount
    date_min = resp['stats']['stats_fields']['standardDate']['min']
    date_max = resp['stats']['stats_fields']['standardDate']['max']
    df.loc[i, 'datasetDateStart'] = date_min.split('T')[0] if date_min else None
    df.loc[i, 'datasetDateEnd'] = date_max.split('T')[0] if date_max else None
    data = resp['facet_counts']['facet_fields']['resourceContacts']
    contact_list = [data[x] for x in range(0, len(data),2)]
    resourceContacts = ';'.join(contact_list)
    df.loc[i,'resourceContacts'] = resourceContacts

# datasetTaxonGroup 
# 先用原本的算法 之後可以改成直接用taxonGroup


for i in df.index:
    if i % 100 == 0:
        print(i)
    row = df.iloc[i]
    taxon_stat = {}
    # 先全部都給 0
    for bb in bio_group_en.keys():
        taxon_stat[bio_group_en[bb]] = 0
    taxon_stat['Others'] = 0
    taxon_string = []
    # for val in taxon_group_map.keys():
    query_list = []
    # 測試先用 rightsHolder + datasetName
    query_list += ['tbiaDatasetID:"{}"'.format(row.tbiaDatasetID)]
    query = { "query": "*:*",
            "offset": 0,
            "limit": 0,
            "filter": query_list,
            } 
    # 資料裡面帶的bioGroup會是中文
    response = requests.post('http://solr:8983/solr/tbia_records/select?facet.field=bioGroup&facet.limit=-1&facet.mincount=1&facet=true', data=json.dumps(query), headers={'content-type': "application/json" })
    resp = response.json()
    data = resp['facet_counts']['facet_fields']['bioGroup']
    for ll in range(0, len(data), 2):
        now_bio_group = data[ll]
        taxon_string.append(now_bio_group)
        taxon_stat[bio_group_en[now_bio_group]] = data[ll+1]
    # 補上其他
    query = { "query": "-bioGroup:*",
            "offset": 0,
            "limit": 0,
            "filter": query_list,
            }
    response = requests.post('http://solr:8983/solr/tbia_records/select?', data=json.dumps(query), headers={'content-type': "application/json" })
    resp = response.json()
    if resp['response'].get('numFound'):
        taxon_stat['Others'] = resp['response'].get('numFound')
        taxon_string.append('其他')
    df.loc[i, 'datasetTaxonGroup'] = (',').join(taxon_string)
    df.loc[i, 'datasetTaxonStat'] = json.dumps(taxon_stat)


df = df.replace({np.nan:None})
# 存入資料庫
conn = psycopg2.connect(**db_settings)

for i in df.index:
    if i % 100 == 0:
        print(i)
    row = df.iloc[i]
    query = '''UPDATE dataset SET "datasetTaxonGroup" = %s, "datasetTaxonStat" = %s, "occurrenceCount" = %s, 
                "datasetDateStart" = %s, "datasetDateEnd" = %s, "resourceContacts" = %s WHERE "tbiaDatasetID" = %s;
            '''
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query, (row.datasetTaxonGroup, row.datasetTaxonStat, row.occurrenceCount, row.datasetDateStart, row.datasetDateEnd, row.resourceContacts, row.tbiaDatasetID))
        conn.commit()



# 從測試站要匯出更新的檔案給正式站
query = "SELECT * FROM dataset;"

with conn.cursor() as cursor:
    cursor.execute(query)
    res = cursor.fetchall()


query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'dataset';"

with conn.cursor() as cursor:
    cursor.execute(query)
    keys = cursor.fetchall()
    keys = [k[0] for k in keys]


df = pd.DataFrame(res, columns=keys)

df = df.drop(columns=['downloadCount'])

now = datetime.now().strftime('%Y%m%d')

df.to_csv('/bucket/updated_dataset_{}.csv'.format(now), index=None)