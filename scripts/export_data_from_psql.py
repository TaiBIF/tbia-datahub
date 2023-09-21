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


with db.begin() as conn:
    qry = sa.text("select * from taxon")
    resultset = conn.execute(qry)
    taxon = resultset.mappings().all()

taxon = pd.DataFrame(taxon)
taxon = taxon.drop(columns=['scientificNameID','id'])


group_list = ['brcas','brmas','cpami','fact','forest','ntm','oca','taif','tcd','tbri','wra'] 
# gbif 另外處理

# 要join taxon的表

# taxonID
# parentTaxonID
# null

# group_list = ['ntm']

# taxonID
for group in group_list:
    limit = 10000
    offset = 0
    has_more_data = True
    while has_more_data:
        results = []
        with db.begin() as conn:
            qry = sa.text("""select * from records  
                          where "group" = '{}' limit {} offset {}  """.format(group, limit, offset)) 
            resultset = conn.execute(qry)
            results = resultset.mappings().all()
        if len(results):
            df = pd.DataFrame(results)
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
            final_df.to_csv(f'/solr/csvs/updated/{group}_{offset}.csv', index=None)
            offset += limit
        if len(results) < limit:
            has_more_data = False

