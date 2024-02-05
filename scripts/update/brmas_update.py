import numpy as np
from numpy import nan
import requests
import pandas as pd
import bson
import time
import os
from datetime import datetime, timedelta

import math 

from dotenv import load_dotenv
import os
load_dotenv(override=True)

from scripts.taxon.match_taibif_utils import matching_flow
from scripts.utils import *

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName',]

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'brmas'
rights_holder = '中央研究院生物多樣性中心植物標本資料庫'

# 在portal.Partner.info裡面的id
info_id = 0


response = requests.get(f'http://solr:8983/solr/tbia_records/select?fl=update_version&fq=rightsHolder:{rights_holder}&q.op=OR&q=*%3A*&rows=1&sort=update_version%20desc')
if response.status_code == 200:
    resp = response.json()
    if data := resp['response']['docs']:
        update_version = data[0]['update_version'] + 1
    else:
        update_version = 1

# 在開始之前 先確認存不存在 
# 若不存在 insert一個新的update_version
current_page, note = insert_new_update_version(rights_holder=rights_holder,update_version=update_version)


url = f"https://hast.biodiv.tw/api/v1/occurrence"
response = requests.get(url, verify=False)
if response.status_code == 200:
    result = response.json()
    total = result['meta']['total']
    total_page = result['meta']['pagination']['num_pages']

now = datetime.now() + timedelta(hours=8) 

for p in range(current_page,total_page,10):
    print(p)
    data = []
    c = p
    while c < p + 10 and c < total_page:
        offset = 300 * c
        print('page:',c , ' , offset:', offset)
        time.sleep(1)
        url = f"https://hast.biodiv.tw/api/v1/occurrence?offset={offset}"
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            result = response.json()
            data += result.get('data')
        c+=1
    df = pd.DataFrame(data)
    df = df.replace({nan: '', None: ''})
    df = df[~((df.isPreferredName=='')&(df.scientificName==''))]
    if 'sensitiveCategory' in df.keys():
        df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]
    if len(df):
        df = df.rename(columns={'created': 'sourceCreated', 'modified': 'sourceModified', 'scientificName': 'sourceScientificName', 
        'isPreferredName': 'sourceVernacularName', 'collectionID': 'catalogNumber', 'taxonRank': 'sourceTaxonRank'})
        df = df.replace({nan: ''})
        sci_names = df[sci_cols].drop_duplicates().reset_index(drop=True)
        sci_names = matching_flow(sci_names)
        df = df.drop(columns=['taxonID'], errors='ignore')
        match_taxon_id = sci_names
        if len(match_taxon_id):
            match_taxon_id = match_taxon_id.replace({nan: ''})
            match_taxon_id[sci_cols] = match_taxon_id[sci_cols].replace({'': '-999999'})
            df[df_sci_cols] = df[df_sci_cols].replace({'': '-999999',None:'-999999'})
            df = df.merge(match_taxon_id, on=df_sci_cols, how='left')
            df[sci_cols] = df[sci_cols].replace({'-999999': ''})
        df['sourceCreated'] = df['sourceCreated'].apply(lambda x: convert_date(x))
        df['sourceModified'] = df['sourceModified'].apply(lambda x: convert_date(x))
        df['group'] = group
        df['rightsHolder'] = rights_holder
        df['created'] = now
        df['modified'] = now
        df['recordType'] = 'col'
        # 出現地
        if 'locality' in df.keys():
            df['locality'] = df['locality'].apply(lambda x: x.strip() if x else x)
        # 日期
        df['standardDate'] = df['eventDate'].apply(lambda x: convert_date(x))
        # 數量 
        df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
        # basisOfRecord 無資料
        # 敏感層級 無資料
        # 經緯度
        # df['grid_1'] = '-1_-1'
        # df['grid_5'] = '-1_-1'
        # df['grid_10'] = '-1_-1'
        # df['grid_100'] = '-1_-1'
        # df['grid_1_blurred'] = '-1_-1'
        # df['grid_5_blurred'] = '-1_-1'
        # df['grid_10_blurred'] = '-1_-1'
        # df['grid_100_blurred'] = '-1_-1'
        df['id'] = ''
        # df['standardLongitude'] = None
        # df['standardLatitude'] = None
        # df['location_rpt'] = None
        for i in df.index:
            # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
            df.loc[i,'id'] = str(bson.objectid.ObjectId())
            row = df.loc[i]
            if 'mediaLicense' in df.keys() and 'associatedMedia' in df.keys():
                if not row.mediaLicense:
                    df.loc[i,'associatedMedia'] = None
            grid_data = create_grid_data(verbatimLongitude=row.verbatimLongitude, verbatimLatitude=row.verbatimLatitude)
            df.loc[i,'standardLongitude'] = grid_data.get('standardLon')
            df.loc[i,'standardLatitude'] = grid_data.get('standardLat')
            df.loc[i,'location_rpt'] = grid_data.get('location_rpt')
            df.loc[i, 'grid_1'] = grid_data.get('grid_1')
            df.loc[i, 'grid_1_blurred'] = grid_data.get('grid_1_blurred')
            df.loc[i, 'grid_5'] = grid_data.get('grid_5')
            df.loc[i, 'grid_5_blurred'] = grid_data.get('grid_5_blurred')
            df.loc[i, 'grid_10'] = grid_data.get('grid_10')
            df.loc[i, 'grid_10_blurred'] = grid_data.get('grid_10_blurred')
            df.loc[i, 'grid_100'] = grid_data.get('grid_100')
            df.loc[i, 'grid_100_blurred'] = grid_data.get('grid_100_blurred')
            # standardLon, standardLat, location_rpt = standardize_coor(row.verbatimLongitude, row.verbatimLatitude)
            # # 因為沒有模糊化座標 所以grid_* & grid_*_blurred 欄位填一樣的
            # if standardLon and standardLat:
            #     df.loc[i,'standardLongitude'] = standardLon
            #     df.loc[i,'standardLatitude'] = standardLat
            #     df.loc[i,'location_rpt'] = location_rpt
            #     grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.01)
            #     df.loc[i, 'grid_1'] = str(int(grid_x)) + '_' + str(int(grid_y))
            #     df.loc[i, 'grid_1_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
            #     grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.05)
            #     df.loc[i, 'grid_5'] = str(int(grid_x)) + '_' + str(int(grid_y))
            #     df.loc[i, 'grid_5_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
            #     grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.1)
            #     df.loc[i, 'grid_10'] = str(int(grid_x)) + '_' + str(int(grid_y))
            #     df.loc[i, 'grid_10_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
            #     grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 1)
            #     df.loc[i, 'grid_100'] = str(int(grid_x)) + '_' + str(int(grid_y))
            #     df.loc[i, 'grid_100_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
        # 資料集
        ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
        update_dataset_key(ds_name=ds_name, rights_holder=rights_holder)
        # 更新match_log
        # 更新資料
        df['occurrenceID'] = df['occurrenceID'].astype('str')
        existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID','datasetName'])
        existed_records = get_existed_records(df['occurrenceID'].to_list(), rights_holder)
        existed_records = existed_records.replace({nan:''})
        if len(existed_records):
            df = df.merge(existed_records,on=["occurrenceID","datasetName"], how='left')
            df = df.replace({nan: None})
            # 如果已存在，取存在的tbiaID
            df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
            df = df.drop(columns=['tbiaID'])
        # match_log要用更新的
        match_log = df[['occurrenceID','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group','rightsHolder','created','modified']]
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
        df['update_version'] = int(update_version)
        # df = df.drop(columns=psql_records_key,errors='ignore')
        df.to_sql('records', db, # schema='my_schema',
                if_exists='append',
                index=False,
                method=records_upsert)
    # 成功之後 更新update_update_version
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=None)


# 刪除is_deleted的records & match_log
delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))

# 打包match_log
zip_match_log(group=group,info_id=info_id)

# 更新update_version
update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)

# 更新 datahub - dataset
# 前面已經處理過新增了 最後只需要處理deprecated的部分
update_dataset_deprecated(rights_holder=rights_holder)


print('done!')