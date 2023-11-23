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
# with db.begin() as conn:
#     qry = sa.text("select * from taxon")
#     resultset = conn.execute(qry)
#     taxon = resultset.mappings().all()

# taxon = pd.DataFrame(taxon)
# taxon = taxon.drop(columns=['scientificNameID','id'])


# url = "http://solr:8983/solr/taxa/select?indent=true&q.op=OR&q=*%3A*&rows=2147483647"
# resp = requests.get(url)
# taxon = resp.json()['response']['docs']


# with db.begin() as conn:
#     qry = sa.text("select * from taxon")
#     resultset = conn.execute(qry)
#     taxon = resultset.mappings().all()

# taxon = pd.DataFrame(taxon)
# taxon = taxon.rename(columns={'id': 'taxonID'})
# taxon = taxon.drop(columns=['taxon_name_id','_version_'])
# taxon = taxon.replace({nan:None})


# group_list = ['brcas','brmas','cpami','fact','forest','ntm','oca','taif','tcd','wra'] 
# gbif tbri 另外處理

# 要join taxon的表

# taxonID
# parentTaxonID
# null

import time

# taxonID

# for group in group_list:

total_count = 0

limit = 10000
offset = 0
min_id = 1
has_more_data = True
rights_holder = ''
group = ''
group_index = 0

while has_more_data:
    s = time.time()
    results = []
    with db.begin() as conn:
        qry = sa.text("""select * from records  
                        where "rightsHolder" = '{}' id > {} order by id limit {}  """.format(rights_holder, min_id, limit)) 
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
            taxon = get_taxon_df(taxon_ids=df[df.taxonID.notnull()].taxonID.to_list())
            # taxonID
            final_df = df.merge(taxon,on='taxonID',how='left')
        else:
            final_df = df
        # a = df[df.taxonID.notnull()].merge(taxon,on='taxonID',)
        # parentTaxonID
        # b = df[df.parentTaxonID.notnull()].drop(columns=['taxonID']).merge(taxon, left_on='parentTaxonID', right_on='taxonID')
        # b['taxonID'] = None
        # null
        # c = df[(df.taxonID.isnull()&df.parentTaxonID.isnull())]
        # final_df = pd.concat([a,b,c],ignore_index=True)
        if len(results) != len(final_df):
            print('error', min_id)
        final_df.to_csv(f'/solr/csvs/export/{group}_{group_index}_export_{offset}.csv', index=None)
        offset += limit
    if len(results) < limit:
        has_more_data = False

print('total_count', total_count)