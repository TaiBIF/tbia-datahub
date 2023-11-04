# 重新比對的時候要抓回原始檔

rights_list = ["台灣生物多樣性網絡 TBN", "生態調查資料庫系統","臺灣國家公園生物多樣性資料庫","臺灣生物多樣性資訊機構 TaiBIF","林業試驗所植物標本資料庫","濕地環境資料庫","林業試驗所昆蟲標本館",
 "河川環境資料庫","中央研究院生物多樣性中心植物標本資料庫","國立臺灣博物館典藏","海洋保育資料倉儲系統","GBIF"]


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


with db.begin() as conn:
    qry = sa.text("select * from taxon")
    resultset = conn.execute(qry)
    taxon = resultset.mappings().all()

taxon = pd.DataFrame(taxon)
taxon = taxon.drop(columns=['scientificNameID','id'])


# group_list = ['brcas','brmas','cpami','fact','forest','ntm','oca','taif','tcd','wra'] 
# gbif tbri 另外處理

# 要join taxon的表

# taxonID
# parentTaxonID
# null

# group_list = ['nps']
import time

# taxonID
r_index = 0

for r in rights_list:
    r_count = 0
    r_index += 1
    limit = 10000
    offset = 0
    min_id = 1
    has_more_data = True
    while has_more_data:
        s = time.time()
        results = []
        with db.begin() as conn:
            qry = sa.text("""select * from records  
                          where "rightsHolder" = '{}' AND id > {} order by id limit {}  """.format(r, min_id, limit)) 
            resultset = conn.execute(qry)
            results = resultset.mappings().all()
        print(time.time()-s, r, offset, min_id)        
        if len(results):
            r_count += len(results)
            df = pd.DataFrame(results)
            # 下一次query最小的id
            min_id = df.id.max()
            df = df.drop(columns=['id'])
            df = df.rename(columns={'tbiaID': 'id'})
            # taxonID
            a = df[df.taxonID.notnull()].merge(taxon,on='taxonID')
            # parentTaxonID
            b = df[df.parentTaxonID.notnull()].drop(columns=['taxonID']).merge(taxon, left_on='parentTaxonID', right_on='taxonID')
            b['taxonID'] = None
            # null
            c = df[(df.taxonID.isnull()&df.parentTaxonID.isnull())]
            final_df = pd.concat([a,b,c],ignore_index=True)
            final_df.to_csv(f'/solr/csvs/export/{r_index}_{offset}.csv', index=None)
            offset += limit
        if len(results) < limit:
            has_more_data = False
    print(r, r_count)