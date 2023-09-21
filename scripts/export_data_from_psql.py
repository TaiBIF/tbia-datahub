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

group_list = ['brcas','brmas','cpami','fact','forest','ntm','oca','taif','tcd','tbri','wra'] 
# gbif 另外處理

# 要join taxon的表

# taxonID
# parentTaxonID
# null

group_list = ['ntm']

# taxonID
for group in group_list:
    limit = 10000
    offset = 0
    has_more_data = True
    while has_more_data:
        results = []
        print('taxonID ', group, offset)
        with db.begin() as conn:
            qry = sa.text("""WITH pre_select AS (select * from records  
                          where "group" = '{}' and "taxonID" is not null limit {} offset {} )
                          SELECT * FROM pre_select
                         left join taxon on pre_select."taxonID" = taxon."taxonID" """.format(group, limit, offset)) # 以防萬一還是寫left
            resultset = conn.execute(qry)
            results = resultset.mappings().all()
        if len(results):
            now = datetime.now()
            df = pd.DataFrame(results)
            df = df.drop(columns=['id'])
            df = df.rename(columns={'tbiaID': 'id'})
            df.to_csv(f'/solr/csvs/updated/{group}_t_{offset}.csv', index=None)
            offset += limit
        if len(results) < limit:
            has_more_data = False

 
for group in group_list:
    limit = 10000
    offset = 0
    has_more_data = True
    while has_more_data:
        print('parentTaxonID ', group, offset)
        with db.begin() as conn:
            # qry = sa.text("""select * from records 
            #             join taxon on taxon."taxonID" = records."parentTaxonID"
            #             where records."group" = '{}' 
            #             limit {} offset {}""".format(group, limit, offset))
            qry = sa.text("""WITH pre_select AS (select * from records where "group" = '{}' and "parentTaxonID" is not null limit {} offset {} )
                          SELECT * FROM pre_select
                          left join taxon on pre_select."parentTaxonID" = taxon."taxonID" """.format(group, limit, offset)) # 以防萬一還是寫left
            resultset = conn.execute(qry)
            results = resultset.mappings().all()
        if len(results):
            now = datetime.now()
            df = pd.DataFrame(results)
            df = df.drop(columns=['id'])
            df = df.rename(columns={'tbiaID': 'id'})
            df.to_csv(f'/solr/csvs/updated/{group}_pt_{offset}.csv', index=None)
            offset += limit
        if len(results) < limit:
            has_more_data = False

 
for group in group_list:
    limit = 10000
    offset = 0
    has_more_data = True
    while has_more_data:
        print('no taxonID ', group, offset)
        with db.begin() as conn:
            qry = sa.text("""select * from records 
                        where records."group" = '{}' and records."parentTaxonID" is null and records."taxonID" is null
                        limit {} offset {}""".format(group, limit, offset))
            resultset = conn.execute(qry)
            results = resultset.mappings().all()
        if len(results):
            now = datetime.now()
            print(len(results))
            df = pd.DataFrame(results)
            df = df.drop(columns=['id'])
            df = df.rename(columns={'tbiaID': 'id'})
            df.to_csv(f'/solr/csvs/updated/{group}_nt_{offset}.csv', index=None)
            offset += limit
        if len(results) < limit:
            has_more_data = False

