import numpy as np
from numpy import nan
import requests
import pandas as pd
import bson
import time
import os
from dateutil import parser
from datetime import datetime, timedelta
import glob

from scripts.taxon.match_taibif_utils import matching_flow, taxon
from scripts.utils import *


# NOTE:「下載所有生物清單」
# https://wetland-db.tcd.gov.tw/#/bioDataPage
# 放在 tbia-volumes/datahub_bucket/ 中

spe_list = pd.read_csv('/bucket/tcd_wetland_species_list.csv',usecols=['VERNACULARNAME','SCIENTIFICNAME','CATEGORYNAME'])
spe_list = spe_list.replace({nan: None})

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
psql_records_key = [k for k in taxon.keys() if k != 'taxonID']


# 單位資訊
group = 'nps'
rights_holder = '濕地環境資料庫'

# 在portal.Partner.info裡面的id
info_id = 1

# 先將records設為is_deleted='t'
with db.begin() as conn:
    qry = sa.text("""update records set is_deleted = 't' where "rightsHolder" = '{}' and "group" = '{}';""".format(rights_holder, group))
    resultset = conn.execute(qry)

now = datetime.now()

total_len = len(spe_list)

for p in range(0,total_len,10):
    data = []
    if p+10 < total_len:
        end_p = p+10
    else:
        end_p = total_len
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
    if len(data):
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
        df = df.replace({nan: '', 'NA': '', '-99999': ''})
        df = df.drop(columns=['CHK_NAME','SAMPLINGPROTOOL'],errors='ignore')
        sci_names = df[sci_cols].drop_duplicates().reset_index(drop=True)
        sci_names = matching_flow(sci_names)
        df = df.drop(columns=['taxonID'], errors='ignore')
        taxon_list = list(sci_names[sci_names.taxonID!=''].taxonID.unique()) + list(sci_names[sci_names.parentTaxonID!=''].parentTaxonID.unique())
        final_taxon = taxon[taxon.taxonID.isin(taxon_list)]
        final_taxon = pd.DataFrame(final_taxon)
        if len(final_taxon):
            match_taxon_id = sci_names.merge(final_taxon)
            # 若沒有taxonID的 改以parentTaxonID串
            match_parent_taxon_id = sci_names.drop(columns=['taxonID']).merge(final_taxon,left_on='parentTaxonID',right_on='taxonID')
            match_parent_taxon_id['taxonID'] = ''
            match_taxon_id = pd.concat([match_taxon_id, match_parent_taxon_id], ignore_index=True)
            # 如果都沒有對到 要再加回來
            match_taxon_id = pd.concat([match_taxon_id,sci_names[~sci_names.sci_index.isin(match_taxon_id.sci_index.to_list())]], ignore_index=True)
            match_taxon_id = match_taxon_id.replace({nan: ''})
            match_taxon_id[sci_cols] = match_taxon_id[sci_cols].replace({'': '-999999'})
        if len(match_taxon_id):
            df[df_sci_cols] = df[df_sci_cols].replace({'': '-999999',None:'-999999'})
            df = df.merge(match_taxon_id, on=df_sci_cols, how='left')
            df[sci_cols] = df[sci_cols].replace({'-999999': ''})
        df = df.reset_index(drop=True)
        df['group'] = group
        df['occurrenceID'] = ''
        df['rightsHolder'] = rights_holder
        df['created'] = now
        df['modified'] = now
        df['recordType'] = 'occ'
        # 日期
        df['standardDate'] = df['eventDate'].apply(lambda x: convert_date(x))
        # 數量
        df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
        # basisOfRecord 無資料
        # dataGeneralizations ['未模糊化' '有模糊化']
        df['dataGeneralizations'] = df['dataGeneralizations'].apply(lambda x: False if x == '未模糊化' else True)
        # 經緯度
        df['grid_1'] = '-1_-1'
        df['grid_5'] = '-1_-1'
        df['grid_10'] = '-1_-1'
        df['grid_100'] = '-1_-1'
        df['id'] = ''
        df['standardLongitude'] = None
        df['standardLatitude'] = None
        df['verbatimSRS'] = None
        df['verbatimCoordinateSystem'] = None
        df['location_rpt'] = None
        for i in df.index:
            df.loc[i,'id'] = str(bson.objectid.ObjectId())
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
        # datasetName 無資料
        # 最後再檢查一次所有欄位
        df = df.replace({nan: None, '': None})
        # 更新match_log
        # 更新資料
        if len(df[df.occurrenceID!='']):
            df['occurrenceID'] = df['occurrenceID'].astype('str')
            with db.begin() as conn:
                qry = sa.text("""select "tbiaID", "occurrenceID", "created" from records  
                                where "rightsHolder" = '{}' AND "occurrenceID" IN {}  """.format(rights_holder, str(df.occurrenceID.to_list()).replace('[','(').replace(']',')')) )
                resultset = conn.execute(qry)
                results = resultset.mappings().all()
                existed_records = pd.DataFrame(results)
            if len(existed_records):
                df =  df.merge(existed_records,on=["occurrenceID"], how='left')
                df = df.replace({nan: None})
                # 如果已存在，取存在的tbiaID
                df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
                # 如果已存在，取存在的建立日期
                df['created'] = df.apply(lambda x: x.created_y if x.tbiaID else now, axis=1)
                df = df.drop(columns=['tbiaID','created_y','created_x'])
        else:
            df['occurrenceID'] = ''
        # match_log要用更新的
        match_log = df[['occurrenceID','id','sourceScientificName','taxonID','parentTaxonID','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group','rightsHolder','created','modified']]
        match_log = match_log.reset_index(drop=True)
        match_log = update_match_log(match_log=match_log, now=now)
        match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{p}.csv',index=None)
        # records要用更新的
        # 已經串回原本的tbiaID，可以用tbiaID做更新
        df['is_deleted'] = False
        df = df.drop(columns=['match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','taxon_name_id','sci_index'],errors='ignore')
        # 最後再一起匯出
        # # 在solr裡 要使用id當作名稱 而非tbiaID
        # df.to_csv(f'/solr/csvs/updated/{group}_{info_id}_{p}.csv', index=False)
        # 存到records裏面
        df = df.rename(columns=({'id': 'tbiaID'}))
        df = df.drop(columns=psql_records_key,errors='ignore')
        df.to_sql('records', db, # schema='my_schema',
                if_exists='append',
                index=False,
                method=records_upsert)


# 刪除is_deleted的records & match_log
delete_records(rights_holder=rights_holder,group=group)

# 打包match_log
zip_match_log(group=group,info_id=info_id)

print('done!')