# 計算資料集統計
import json
import requests
import psycopg2
import pandas as pd
from datetime import datetime
from tqdm import tqdm

from app import db_settings
from solr_helper import SOLR_BASE, solr_reimport_from_csv


bio_group_en = {
    '鳥類':       'Birds',
    '爬蟲類':     'Reptiles',
    '哺乳類':     'Mammals',
    '甲蟲類':     'Beetles',
    '魚類':       'Fishes',
    '兩棲類':     'Amphibians',
    '蛾類':       'Moths',
    '蝶類':       'Butterflies',
    '蜘蛛':       'Spiders',
    '蜻蛉類':     'Dragonflies',
    '蝸牛與貝類': 'Snails & Shells',
    '其他昆蟲':   'Other Insects',
    '蝦蟹類':     'Crustaceans',
    '裸子植物':   'Gymnosperms',
    '被子植物':   'Angiosperms',
    '蕨類植物':   'Ferns',
    '苔蘚植物':   'Mosses',
    '真菌':       'Fungi',
    '維管束植物': 'Vascular Plants',
    '昆蟲':       'Insects',
    '藻類':       'Algae',
    '病毒':       'Viruses',
    '細菌':       'Bacteria',
}


# 取得所有未棄用的資料集
conn = psycopg2.connect(**db_settings)
query = """SELECT "tbiaDatasetID", "name", "rights_holder" FROM dataset WHERE deprecated = 'f';"""
with conn.cursor() as cursor:
    cursor.execute(query)
    res = cursor.fetchall()
    df = pd.DataFrame(res, columns=['tbiaDatasetID', 'datasetName', 'rights_holder'])


# 每個 dataset 查一次 Solr,一次取得所有需要的 facet 與 stats
records = []
# for i, row in tqdm(df.iterrows(), total=len(df), desc='fetch dataset stats'):
for i, row in df.iterrows():
    params = {
        'q': '*:*',
        'fq': f'tbiaDatasetID:"{row.tbiaDatasetID}"',
        'rows': 0,
        'stats': 'true',
        'stats.field': 'standardDate',
        'facet': 'true',
        'facet.limit': -1,
        'facet.mincount': 1,
        'facet.field': ['resourceContacts', 'recordType', 'bioGroup'],
    }
    resp = requests.get(f'{SOLR_BASE}/tbia_records/select', params=params).json()
    occurrence_count = resp['response']['numFound']
    date_stats = resp['stats']['stats_fields']['standardDate']
    date_min = date_stats.get('min')
    date_max = date_stats.get('max')
    # facet 回傳格式為 [value, count, value, count, ...]
    facets = resp['facet_counts']['facet_fields']
    contacts = facets['resourceContacts'][::2]
    record_types = facets['recordType'][::2]
    # bioGroup → taxon_stat / taxon_string
    taxon_stat = {en: 0 for en in bio_group_en.values()}
    # taxon_stat['Others'] = 0   # 2026-05 已不再填充
    taxon_string = []
    bio_data = facets['bioGroup']
    for k in range(0, len(bio_data), 2):
        zh, count = bio_data[k], bio_data[k + 1]
        taxon_string.append(zh)
        if zh in bio_group_en:
            taxon_stat[bio_group_en[zh]] = count
    records.append({
        'tbiaDatasetID': row.tbiaDatasetID,
        'occurrenceCount': occurrence_count,
        'datasetDateStart': date_min.split('T')[0] if date_min else None,
        'datasetDateEnd': date_max.split('T')[0] if date_max else None,
        'resourceContacts': ';'.join(contacts),
        'record_type': ','.join(record_types),
        'datasetTaxonGroup': ','.join(taxon_string),
        'datasetTaxonStat': json.dumps(taxon_stat),
    })


# 一次批次寫回 PostgreSQL
update_sql = '''
    UPDATE dataset SET
        "datasetTaxonGroup" = %(datasetTaxonGroup)s,
        "datasetTaxonStat"  = %(datasetTaxonStat)s,
        "occurrenceCount"   = %(occurrenceCount)s,
        "datasetDateStart"  = %(datasetDateStart)s,
        "datasetDateEnd"    = %(datasetDateEnd)s,
        "resourceContacts"  = %(resourceContacts)s,
        "record_type"       = %(record_type)s
    WHERE "tbiaDatasetID" = %(tbiaDatasetID)s;
'''
with conn.cursor() as cursor:
    cursor.executemany(update_sql, records)

conn.commit()


# 從測試站匯出更新檔案給正式站
with conn.cursor() as cursor:
    cursor.execute("SELECT * FROM dataset;")
    res = cursor.fetchall()

keys = [
    'id', 'name', 'record_type', 'rights_holder', 'deprecated',
    'datasetTaxonGroup', 'resourceContacts', 'sourceDatasetID',
    'gbifDatasetID', 'tbiaDatasetID', 'occurrenceCount', 'datasetURL',
    'datasetLicense', 'created', 'modified', 'datasetPublisher',
    'update_version', 'datasetDateStart', 'datasetDateEnd',
    'datasetTaxonStat', 'downloadCount', 'group',
]
df = pd.DataFrame(res, columns=keys)
df = df.drop(columns=['downloadCount'])

# 標記同名資料集(未棄用且 name 重複)
dd = df[df.deprecated == False]
dd_names = dd[dd.name.duplicated()].name.unique()
df['is_duplicated_name'] = False
df.loc[df.name.isin(dd_names) & (df.deprecated == False), 'is_duplicated_name'] = True

now = datetime.now().strftime('%Y%m%d')
updating_csv = f'/bucket/tbia_updated_dataset_{now}.csv'
df.to_csv(updating_csv, index=None)


# 刪除 Solr dataset core 並從 CSV 重匯
solr_reimport_from_csv('dataset', updating_csv)