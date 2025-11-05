import numpy as np
from app import db, db_settings
import psycopg2
import pandas as pd


# updating_csv = ("/bucket/tbia_updated_dataset_20250930.csv")
updating_csv = ("/bucket/tbia_dataset_corrected_202509.csv")

df = pd.read_csv(updating_csv)

df = df.replace({np.nan: None, 'nan': None})

# id, occurrenceCount, update_version
df[['id','occurrenceCount','update_version']] = df[['id','occurrenceCount','update_version']].astype(int)
df[['id','occurrenceCount','update_version']] = df[['id','occurrenceCount','update_version']].astype(str)
df['deprecated'] = df['deprecated'].replace({True: 't', False: 'f'})
df['datasetTaxonStat'] = df['datasetTaxonStat'].apply(lambda x: str(x) if x else '')
df['datasetTaxonStat'] = df['datasetTaxonStat'].apply(lambda x: x.replace("'",'"'))
df['datasetTaxonStat'] = df['datasetTaxonStat'].replace({'': None})

# 把刪除 & 重新匯入也寫進這段程式碼

import subprocess

commands = f''' curl http://solr:8983/solr/dataset/update/?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>*:*</query></delete>'; ''' 
process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
a = process.communicate()


commands = f''' curl http://solr:8983/solr/dataset/update/?commit=true -H "Content-Type: text/csv" --data-binary @{updating_csv}; ''' 
process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
a = process.communicate()


df = df.drop(columns=['is_duplicated_name'])

for i in df.index:
    if i % 100 == 0:
        print(i)
    row = df.iloc[i]
    conn = psycopg2.connect(**db_settings)
    query = """
    INSERT INTO dataset ("id", "name", "record_type", "rights_holder", "deprecated",
       "datasetTaxonGroup", "resourceContacts", "sourceDatasetID",
       "gbifDatasetID", "tbiaDatasetID", "occurrenceCount", "datasetURL",
       "datasetLicense", "created", "modified", "datasetPublisher",
       "update_version", "datasetDateStart", "datasetDateEnd",
       "datasetTaxonStat", "group") 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO UPDATE 
    SET "id" = EXCLUDED.id,
        "name" = EXCLUDED."name",
        "record_type" = EXCLUDED."record_type",
        "rights_holder" = EXCLUDED."rights_holder",
        "deprecated" = EXCLUDED."deprecated",
        "datasetTaxonGroup" = EXCLUDED."datasetTaxonGroup",
        "resourceContacts" = EXCLUDED."resourceContacts",
        "sourceDatasetID" = EXCLUDED."sourceDatasetID",
        "gbifDatasetID" = EXCLUDED."gbifDatasetID",
        "tbiaDatasetID" = EXCLUDED."tbiaDatasetID",
        "occurrenceCount" = EXCLUDED."occurrenceCount",
        "datasetURL" = EXCLUDED."datasetURL",
        "datasetLicense" = EXCLUDED."datasetLicense",
        "created" = EXCLUDED."created",
        "modified" = EXCLUDED."modified",
        "datasetPublisher" = EXCLUDED."datasetPublisher",
        "update_version" = EXCLUDED."update_version",
        "datasetDateStart" = EXCLUDED."datasetDateStart",
        "datasetDateEnd" = EXCLUDED."datasetDateEnd",
        "datasetTaxonStat" = EXCLUDED."datasetTaxonStat",
        "group" = EXCLUDED."group"
    """
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query, row.to_list())
        conn.commit()

