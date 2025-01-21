# 重新比對的時候要抓回原始檔

# import requests
import pandas as pd

from numpy import nan
from app import db
import pandas as pd

# from app import portal_db_settings
# 取得taxon資料
# import psycopg2
# import requests
# import re
# import urllib
# import numpy as np
from datetime import datetime, timedelta
import sqlalchemy as sa
import time

from scripts.taxon.match_utils import matching_flow_new
from scripts.utils import *


# with db.begin() as conn:
sci_col_map = {
    '台灣生物多樣性網絡 TBN':  {'sci_cols': ['sourceScientificName','sourceVernacularName','originalVernacularName','sourceTaxonID','scientificNameID','sourceFamily'],
                            'group': 'tbri',
                        'info_id': 0},
    '生態調查資料庫系統': {'sci_cols': ['sourceScientificName','sourceVernacularName'],
                        'group': 'forest',
                        'info_id': 0},
    '濕地環境資料庫': {'sci_cols': ['sourceScientificName','sourceVernacularName','scientificNameID'],
                    'group': 'nps',
                    'info_id': 1,},
    '中央研究院生物多樣性中心植物標本資料庫': {'sci_cols': ['sourceScientificName','sourceVernacularName'],
                                        'group': 'hast',
                                        'info_id': 0},
    'GBIF': {'sci_cols': ['taxonID','sourceVernacularName', 'sourceScientificName','originalVernacularName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily'],
             'group': 'gbif',
              'info_id': 0},
    '林業試驗所植物標本資料庫': {'sci_cols': ['sourceScientificName','sourceVernacularName'],
                             'group': 'taif',
                             'info_id': 0},
    '國立臺灣博物館典藏': {'sci_cols': ['taxonID','sourceVernacularName', 'sourceScientificName','originalVernacularName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily'],
                        'group': 'ntm',
                        'info_id': 0},
    '臺灣生物多樣性資訊機構 TaiBIF': {'sci_cols': ['taxonID','sourceVernacularName', 'sourceScientificName','originalVernacularName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily'],
                                   'group': 'brcas',
                                   'info_id': 0},
    '林業試驗所昆蟲標本館': {'sci_cols': ['sourceScientificName','sourceVernacularName'],
                          'group': 'fact',
                          'info_id': 0},
    '河川環境資料庫': {'sci_cols': ['taxonID','sourceVernacularName', 'sourceScientificName','originalVernacularName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily'],
                    'group': 'wra',
                    'info_id': 0},
    '海洋保育資料倉儲系統': {'sci_cols': ['sourceVernacularName','sourceScientificName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily'],
                         'group': 'oca',
                         'info_id': 0},
    '臺灣國家公園生物多樣性資料庫': {'sci_cols': ['sourceScientificName','sourceVernacularName'],
                                'group': 'nps',
                                'info_id': 1},
}


now = datetime.now() + timedelta(hours=8) 

print(now)

rights_list = list(sci_col_map.keys())

for r in rights_list:
    sci_cols = sci_col_map[r]['sci_cols']
    group = sci_col_map[r]['group']
    info_id = sci_col_map[r]['info_id']
    df_sci_cols = [s for s in sci_cols if s != 'taxonID']
    r_count = 0
    limit = 10000
    offset = 0
    min_id = 0
    has_more_data = True
    p = 0
    while has_more_data:
        p+=1
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
            df = df.drop(columns=['match_higher_taxon','id'])
            # 重新比對學名
            sci_names = df[sci_cols].drop_duplicates().reset_index(drop=True)
            sci_names = matching_flow_new(sci_names)
            df = df.drop(columns=['taxonID'], errors='ignore')
            match_taxon_id = sci_names
            if len(match_taxon_id):
                match_taxon_id = match_taxon_id.replace({nan: ''})
                match_taxon_id[sci_cols] = match_taxon_id[sci_cols].replace({'': '-999999'})
                df[df_sci_cols] = df[df_sci_cols].replace({'': '-999999',None:'-999999'})
                df = df.merge(match_taxon_id, on=df_sci_cols, how='left')
                df[sci_cols] = df[sci_cols].replace({'-999999': ''})
            # 更新match_log
            # match_log = df[['occurrenceID','tbiaID','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group','rightsHolder','created','modified']]
            match_log = df[['occurrenceID','tbiaID','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','group','rightsHolder','created','modified']]
            match_log = match_log.reset_index(drop=True)
            match_log = update_match_log(match_log=match_log, now=now)
            match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{p}.csv',index=None)
            # 更新records
            df['modified'] = now
            # df = df.drop(columns=['match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','taxon_name_id','sci_index'],errors='ignore')
            df = df.drop(columns=['match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','taxon_name_id','sci_index', 'datasetURL','gbifDatasetID', 'gbifID'],errors='ignore')
            # df = df.rename(columns=({'id': 'tbiaID'}))
            df.to_sql('records', db, # schema='my_schema',
                    if_exists='append',
                    index=False,
                    method=records_upsert)
            offset += limit
        if len(results) < limit:
            has_more_data = False
    # 打包match_log
    zip_match_log(group=group,info_id=info_id)

print('done!')

