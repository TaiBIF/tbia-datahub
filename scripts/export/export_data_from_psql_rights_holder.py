
# 重新比對的時候要抓回原始檔

import requests
import pandas as pd

from numpy import nan
from app import db

# 2023-09-04 重新比對學名
import pandas as pd

from app import portal_db_settings
# 取得taxon資料
import psycopg2
import requests
import re
import urllib
import numpy as np
from datetime import datetime, timedelta
import sqlalchemy as sa

from scripts.utils import get_taxon_df

import time

r_list = [
'GBIF',
'中央研究院生物多樣性中心植物標本資料庫',
'台灣生物多樣性網絡 TBN',
'國立臺灣博物館典藏',
'林業試驗所昆蟲標本館',
'林業試驗所植物標本資料庫',
'河川環境資料庫',
'濕地環境資料庫',
'生態調查資料庫系統',
'臺灣國家公園生物多樣性資料庫',
'臺灣生物多樣性資訊機構 TaiBIF',
'海洋保育資料倉儲系統'
]

r_index = 0

for r in r_list:
    total_count = 0
    r_index += 1
    limit = 10000
    offset = 0
    min_id = 0
    has_more_data = True
    while has_more_data:
        s = time.time()
        results = []
        with db.begin() as conn:
            qry = sa.text("""select * from records  
                            where id > {} and "rightsHolder" = '{}' order by id limit {}  """.format(min_id, r, limit)) 
            resultset = conn.execute(qry)
            results = resultset.mappings().all()
        print(time.time()-s, offset, min_id)        
        if len(results):
            total_count += len(results)
            df = pd.DataFrame(results)
            # 下一次query最小的id
            min_id = df.id.max()
            df = df.drop(columns=['id'])
            df = df.rename(columns={'tbiaID': 'id'})
            if len(df[df.taxonID.notnull()]):
                taxon = get_taxon_df(taxon_ids=df[df.taxonID.notnull()].taxonID.unique())
                # taxonID
                if len(taxon):
                    final_df = df.merge(taxon,on='taxonID',how='left')
                else:
                    final_df = df
            else:
                final_df = df
            if len(results) != len(final_df):
                print('error', min_id)
            final_df = final_df.rename(columns={'originalVernacularName': 'originalScientificName'})
            final_df.to_csv(f'/solr/csvs/export/{r_index}_export_{offset}.csv', index=None)
            offset += limit
        if len(results) < limit:
            has_more_data = False

print('total_count', total_count)