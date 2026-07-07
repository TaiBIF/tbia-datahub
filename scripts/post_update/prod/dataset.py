
# TODO 執行前先備份正式站dataset資料庫
# TODO 2026-07 待確認修改後是否正確
# 使用方式
# python ./scripts/prod/dataset.py /path/to/dataset.csv

import argparse

import numpy as np
import psycopg2
import pandas as pd

from app import db_settings
from scripts.post_update.solr_helper import solr_reimport_from_csv


parser = argparse.ArgumentParser(description='Update production dataset from CSV')
parser.add_argument('csv', help='Path to the dataset CSV (e.g. /bucket/tbia_updated_dataset_20260331.csv)')
args = parser.parse_args()

updating_csv = args.csv


df = pd.read_csv(updating_csv)
df = df.replace({np.nan: None, 'nan': None})

# dtype 清理
df[['id', 'occurrenceCount', 'update_version']] = (
    df[['id', 'occurrenceCount', 'update_version']].astype(int).astype(str)
)
df['deprecated'] = df['deprecated'].replace({True: 't', False: 'f'})
df['datasetTaxonStat'] = df['datasetTaxonStat'].apply(
    lambda x: str(x).replace("'", '"') if x else None
)


# 1. 刪除 Solr dataset core 並從 CSV 重匯
solr_reimport_from_csv('dataset', updating_csv)


# 2. 更新 / 新增 PostgreSQL(id 存在就 update,不存在就 insert)
df = df.drop(columns=['is_duplicated_name'])
df = df[df.record_type.notnull()].reset_index(drop=True)

columns = [
    'id', 'name', 'record_type', 'rights_holder', 'deprecated',
    'datasetTaxonGroup', 'resourceContacts', 'sourceDatasetID',
    'gbifDatasetID', 'tbiaDatasetID', 'occurrenceCount', 'datasetURL',
    'datasetLicense', 'created', 'modified', 'datasetPublisher',
    'update_version', 'datasetDateStart', 'datasetDateEnd',
    'datasetTaxonStat', 'group',
]

records = df[columns].to_dict('records')

col_list = ', '.join(f'"{c}"' for c in columns)
placeholders = ', '.join(f'%({c})s' for c in columns)
update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in columns if c != 'id')

upsert_sql = f'''
    INSERT INTO dataset ({col_list})
    VALUES ({placeholders})
    ON CONFLICT ("id") DO UPDATE SET {update_set};
'''

conn = psycopg2.connect(**db_settings)
try:
    with conn.cursor() as cursor:
        cursor.executemany(upsert_sql, records)
        # 剛剛用 explicit id 硬塞進去,把 serial 序列推到目前最大值,避免之後 auto id 撞號
        cursor.execute(
            "SELECT setval(pg_get_serial_sequence('dataset', 'id'), "
            "COALESCE((SELECT MAX(id) FROM dataset), 1));"
        )
    conn.commit()
finally:
    conn.close()