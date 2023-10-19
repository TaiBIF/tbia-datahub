# 2023-10-19
# RUN in app container
# script for 營建署城鄉分署 濕地環境 API 
# from django.db import connection
# import urllib.parse
import numpy as np
# import bisect
# import re
# from numpy import nan
import requests
import pandas as pd
import bson
import time
import os
# from conf.settings import env
from dateutil import parser
from datetime import datetime, timedelta
import glob
# from data.models import Namecode, Taxon, DatasetKey
from sqlalchemy import create_engine
import psycopg2

# from scripts.data_prep.utils import *
from scripts.taxon.match_taibif_utils import matching_flow, taxon
from scripts.utils import *

psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 「下載所有生物清單」
# https://wetland-db.tcd.gov.tw/#/bioDataPage
# 放在 tbia-volumes/datahub_bucket/ 中

group = 'nps'
rights_holder = '濕地環境資料庫'

## 在portal.Partner.info裡面的id
info_id = 1

spe_list = pd.read_csv('/bucket/tcd_wetland_species_list.csv',usecols=['VERNACULARNAME','SCIENTIFICNAME','CATEGORYNAME'])
spe_list = spe_list.replace({np.nan: None})

# 濕地資料庫
# 刪除records, match_log (用rights_holder)
with db.begin() as conn:
    qry = sa.text("""delete from records where "rightsHolder" = '{}' and "group" = '{}';""".format(rights_holder, group))
    resultset = conn.execute(qry)

with db.begin() as conn:
    qry = sa.text("""delete from match_log where "rights_holder" = '{}' and "group" = '{}';""".format(rights_holder, group))
    resultset = conn.execute(qry)


for p in range(0,len(spe_list),10):
    data = []
    if p+10 < len(spe_list):
        end_p = p+10
    else:
        end_p = len(spe_list)
    print(p, end_p)
    for spe_name in spe_list.SCIENTIFICNAME.unique()[p:end_p]: # 3824
        # time.sleep(60)
        # c += 1
        # print(c)
        offset = 0
        has_next = True
        while has_next:
            print(spe_name, offset)
            url = f"https://wetland-db.tcd.gov.tw/wlfea/RESTful/OpenAPI/GetBioData?name={spe_name}&limit=1000&offset={offset}"
            response = requests.get(url)
            if response.status_code == 200:
                try:
                    result = response.json()
                    data += result
                    if len(data) < 1000:
                        has_next = False
                    else:
                        has_next = True
                        offset += 1000
                except:
                    has_next = False
                    break
    df = pd.DataFrame(data)
    df = df.rename(columns={'LONGITUDE': 'verbatimLongitude',
                            'LATITUDE': 'verbatimLatitude',
                            'EVENTDATE': 'eventDate',
                            'VERNACULARNAME': 'sourceVernacularName',
                            'RECORDEDBY': 'recordedBy',
                            'ORGANISMQUANTITY': 'organismQuantity',
                            'ORGANISMQUANTITYTYPE': 'organismQuantityType',
                            'NAME': 'sourceScientificName',
                            'BLURRED': 'dataGeneralizations',
                            'TAXONRANK': 'sourceTaxonRank'})
    df = df.reset_index(drop=True)
    df = df.replace({np.nan: '', 'NA': '', '-99999': ''})
    df = df.drop(columns=['CHK_NAME','SAMPLINGPROTOOL'])
    sci_names = df[['sourceScientificName','sourceVernacularName']].drop_duplicates().reset_index(drop=True)
    sci_names['sci_index'] = sci_names.index
    sci_names['taxonID'] = ''
    sci_names['parentTaxonID'] = ''
    sci_names['match_stage'] = 1
    # 各階段的issue default是沒有對到
    sci_names['stage_1'] = 2
    sci_names['stage_2'] = 2
    sci_names['stage_3'] = 2
    sci_names['stage_4'] = 2
    sci_names['stage_5'] = 2
    sci_names = matching_flow(sci_names)
    taxon_list = list(sci_names[sci_names.taxonID!=''].taxonID.unique()) + list(sci_names[sci_names.parentTaxonID!=''].parentTaxonID.unique())
    final_taxon = taxon[taxon.taxonID.isin(taxon_list)]
    final_taxon = pd.DataFrame(final_taxon)
    if len(final_taxon):
        # final_taxon = final_taxon.drop(columns=['id'])
        # final_taxon = final_taxon.rename(columns={'scientificNameID': 'taxon_name_id'})
        # sci_names = sci_names.rename(columns={'scientificName': 'sourceScientificName'})
        # sci_names['sci_index'] = sci_names.index
        match_taxon_id = sci_names.merge(final_taxon)
        # 若沒有taxonID的 改以parentTaxonID串
        match_parent_taxon_id = sci_names.drop(columns=['taxonID']).merge(final_taxon,left_on='parentTaxonID',right_on='taxonID')
        match_parent_taxon_id['taxonID'] = ''
        match_taxon_id = pd.concat([match_taxon_id, match_parent_taxon_id], ignore_index=True)
        # match_taxon_id = match_taxon_id.append(match_parent_taxon_id,ignore_index=True)
        # 如果都沒有對到 要再加回來
        # match_taxon_id = match_taxon_id.append(sci_names[~sci_names.sci_index.isin(match_taxon_id.sci_index.to_list())],ignore_index=True)
        match_taxon_id = pd.concat([match_taxon_id,sci_names[~sci_names.sci_index.isin(match_taxon_id.sci_index.to_list())]], ignore_index=True)
        match_taxon_id = match_taxon_id.replace({np.nan: ''})
        match_taxon_id[['sourceScientificName','sourceVernacularName']] = match_taxon_id[['sourceScientificName','sourceVernacularName']].replace({'': '-999999'})
    if len(match_taxon_id):
        df[['sourceScientificName','sourceVernacularName']] = df[['sourceScientificName','sourceVernacularName']].replace({'': '-999999',None:'-999999'})
        df = df.merge(match_taxon_id, on=['sourceScientificName','sourceVernacularName'], how='left')
        df[['sourceScientificName','sourceVernacularName']] = df[['sourceScientificName','sourceVernacularName']].replace({'-999999': ''})
    df = df.reset_index(drop=True)
    df['group'] = group
    df['occurrenceID'] = ''
    df['rightsHolder'] = rights_holder
    df['created'] = datetime.now()
    df['modified'] = datetime.now()
    df['recordType'] = 'occ'
    # 日期
    df['standardDate'] = df['eventDate'].apply(lambda x: convert_date(x))
    # 數量
    df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
    # 經緯度
    df['grid_1'] = '-1_-1'
    df['grid_5'] = '-1_-1'
    df['grid_10'] = '-1_-1'
    df['grid_100'] = '-1_-1'
    for i in df.index:
        df.loc[i,'id'] = bson.objectid.ObjectId()
        row = df.loc[i]
        standardLon, standardLat, location_rpt = standardize_coor(row.verbatimLongitude, row.verbatimLatitude)
        if standardLon and standardLat:
            df.loc[i,'standardLongitude'] = standardLon
            df.loc[i,'standardLatitude'] = standardLat
            df.loc[i,'location_rpt'] = location_rpt
            df.loc[i,'verbatimSRS'] = 'WGS84'
            df.loc[i,'verbatimCoordinateSystem'] = 'DecimalDegrees'
            grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.01)
            df.loc[i, 'grid_1'] = str(int(grid_x)) + '_' + str(int(grid_y))
            grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.05)
            df.loc[i, 'grid_5'] = str(int(grid_x)) + '_' + str(int(grid_y))
            grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.1)
            df.loc[i, 'grid_10'] = str(int(grid_x)) + '_' + str(int(grid_y))
            grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 1)
            df.loc[i, 'grid_100'] = str(int(grid_x)) + '_' + str(int(grid_y))
    # basisOfRecord 無資料
    # dataGeneralizations
    print(df['dataGeneralizations'].unique())
    df['dataGeneralizations'] = df['dataGeneralizations'].apply(lambda x: False if x == '未模糊化' else True)
    # datasetName 無資料
    # 最後再檢查一次所有欄位
    match_log = df[['occurrenceID','id','sourceScientificName','taxonID','parentTaxonID','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group','rightsHolder','created','modified']]
    match_log.loc[match_log.taxonID=='','is_matched'] = False
    match_log.loc[(match_log.taxonID!='')|(match_log.parentTaxonID!=''),'is_matched'] = True
    match_log = match_log.replace({np.nan: None})
    match_log['match_stage'] = match_log['match_stage'].apply(lambda x: int(x) if x else None)
    match_log['stage_1'] = match_log['stage_1'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_2'] = match_log['stage_2'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_3'] = match_log['stage_3'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_4'] = match_log['stage_4'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_5'] = match_log['stage_5'].apply(lambda x: issue_map[x] if x else x)
    match_log['group'] = group
    match_log = match_log.rename(columns={'id': 'tbiaID','rightsHolder':'rights_holder'})
    match_log['tbiaID'] = match_log['tbiaID'].apply(lambda x: str(x))
    # conn_string = env('DATABASE_URL').replace('postgres://', 'postgresql://')
    # db = create_engine(conn_string)
    match_log.to_sql('match_log', db, if_exists='append',schema='public', index=False)
    match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{p}.csv',index=None)
    # df = df.rename(columns={'taxon_name_id': 'scientificNameID'})
    df = df.drop(columns=['match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','taxon_name_id','sci_index'],errors='ignore')
    df['is_deleted'] = False
    # 在solr裡 要使用id當作名稱 而非tbiaID
    df.to_csv(f'/solr/csvs/updated/{group}_{info_id}_{p}.csv', index=False)
    # 存到records裏面
    df = df.rename(columns=({'id': 'tbiaID'}))
    df['tbiaID'] = df['tbiaID'].apply(lambda x: str(x))
    df = df.drop(columns=psql_records_key,errors='ignore')
    df.to_sql('records', db, if_exists='append',schema='public', index=False)



# sql = """
# copy (
#     SELECT mm."tbiaID", mm."occurrenceID", mm."sourceScientificName", mm."taxonID",
#     mm."parentTaxonID", mm.is_matched, dt."scientificName", dt."taxonRank",
#     mm.match_stage, mm.stage_1, mm.stage_2, mm.stage_3, mm.stage_4, mm.stage_5
#     FROM manager_matchlog mm
#     LEFT JOIN data_taxon dt ON mm."taxonID" = dt."taxonID"
#     WHERE mm."rights_holder" = '{}'
# ) to stdout with delimiter ',' csv header;
# """.format(rights_holder)
# with connection.cursor() as cursor:
#     with open(f'/tbia-volumes/media/match_log/{group}_{info_id}_match_log.csv', 'w+') as fp:
#         cursor.copy_expert(sql, fp)

import subprocess
zip_file_path = f'/portal/media/match_log/{group}_{info_id}_match_log.zip'
csv_file_path = f'{group}_{info_id}_*.csv'
commands = f"cd /portal/media/match_log/; zip -j {zip_file_path} {csv_file_path}; rm {csv_file_path}"
process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 等待檔案完成
process.communicate()


print('done!')
# 新增records, match_log (用rights_holder)
# 打包match_log檔案 (用rights_holder)
# # 刪除solr中的資料 (用rights_holder)
# 匯入solr (csv的檔案名稱選擇和match_log邏輯一致)


