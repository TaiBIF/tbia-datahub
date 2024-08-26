import numpy as np
from numpy import nan
import requests
import pandas as pd
import bson
import time
from datetime import datetime, timedelta

import math 

from dotenv import load_dotenv
import os
load_dotenv(override=True)

from scripts.taxon.match_utils import matching_flow
from scripts.utils import *


# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'forest'
rights_holder = '生態調查資料庫系統'

# 在portal.Partner.info裡面的id
info_id = 0

# 排除重複資料集
duplicated_dataset_list = ['102年度臺灣兩棲類資源調查與教育宣導推廣計畫','103年度台灣兩棲類資源調查與教育推廣計畫','104年度台灣兩棲類資源調查與教育推廣計畫','105年度台灣兩棲類資源調查與教育推廣計畫','外來種斑腿樹蛙控制與監測計畫']

# # 先將records設為is_deleted='t'
# with db.begin() as conn:
#     qry = sa.text("""update records set is_deleted = 't' where "rightsHolder" = '{}' and "group" = '{}';""".format(rights_holder, group))
#     resultset = conn.execute(qry)

response = requests.get(f'http://solr:8983/solr/tbia_records/select?fl=update_version&fq=rightsHolder:"{rights_holder}"&q.op=OR&q=*%3A*&rows=1&sort=update_version%20desc')
if response.status_code == 200:
    resp = response.json()
    if data := resp['response']['docs']:
        update_version = data[0]['update_version'] + 1
    else:
        update_version = 1

# 在開始之前 先確認存不存在 
# 若不存在 insert一個新的update_version
current_page, note = insert_new_update_version(rights_holder=rights_holder,update_version=update_version)

url = f"https://ecollect.forest.gov.tw/EcologicalTBiAOpenApi/api/Data/Get?Token={os.getenv('FOREST_KEY')}"
response = requests.get(url)
if response.status_code == 200:
    result = response.json()
    total_page = result['Meta']['TotalPages']

now = datetime.now() + timedelta(hours=8)

for p in range(current_page,total_page,10):
# for p in [0]:
    print(p)
    data = []
    c = p
    while c < p + 10 and c < total_page:
        c+=1
        print('page:',c)
        # time.sleep(30)
        url = f"https://ecollect.forest.gov.tw/EcologicalTBiAOpenApi/api/Data/Get?Token={os.getenv('FOREST_KEY')}&Page={c}"
        response = requests.get(url)
        if response.status_code == 200:
            result = response.json()
            total_page = result['Meta']['TotalPages']
            data += result.get('Data')
    df = pd.DataFrame(data)
    # 如果 'isPreferredName','scientificName',都是空值才排除
    df = df.replace({nan: '', None: '', 'NA': '', '-99999': '', 'N/A': ''})
    df = df[~((df.isPreferredName=='')&(df.scientificName==''))]
    # df = df[~(df.isPreferredName.isin([nan,'',None])&df.scientificName.isin([nan,'',None]))]
    # 排除重複資料集
    df = df[~df.datasetName.isin(duplicated_dataset_list)]
    if 'sensitiveCategory' in df.keys():
        df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]
    if 'license' in df.keys():
        df = df[(df.license!='')&(~df.license.str.contains('BY NC ND|BY-NC-ND',regex=True))]
    else:
        df = []
    media_rule_list = []
    if len(df):
        df = df.reset_index(drop=True)
        df = df.replace({nan: '', None: '', 'NA': '', '-99999': '', 'N/A': ''})
        df = df.rename(columns={'created': 'sourceCreated', 'modified': 'sourceModified', 'scientificName': 'sourceScientificName',
                                'isPreferredName': 'sourceVernacularName', 'taxonRank': 'sourceTaxonRank'})
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
        # 出現地
        if 'locality' in df.keys():
            df['locality'] = df['locality'].apply(lambda x: x.strip() if x else x)
        # 日期
        df['standardDate'] = df['eventDate'].apply(lambda x: convert_date(x))
        # 數量
        df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
        # basisOfRecord
        df['recordType'] = df.apply(lambda x: 'col' if '標本' in x.basisOfRecord else 'occ', axis=1)
        df['basisOfRecord'] = df['basisOfRecord'].apply(lambda x: control_basis_of_record(x))
        # dataGeneralizations
        df['dataGeneralizations'] = df['dataGeneralizations'].replace({'N': False, 'Y': True})
        df['id'] = ''
        for i in df.index:
            # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
            df.loc[i,'id'] = str(bson.objectid.ObjectId())
            row = df.loc[i]
            if 'mediaLicense' in df.keys() and 'associatedMedia' in df.keys():
                if not row.mediaLicense:
                    df.loc[i,'associatedMedia'] = None
                if df.loc[i, 'associatedMedia']:
                    media_rule = get_media_rule(df.loc[i, 'associatedMedia'])
                    if media_rule and media_rule not in media_rule_list:
                        media_rule_list.append(media_rule)
            # 2023-05-24 改成直接回傳未模糊化座標
            try:
                coordinatePrecision = float(row.coordinatePrecision)
                df.loc[i,'dataGeneralizations'] = True
            except:
                coordinatePrecision = None
            grid_data = create_blurred_grid_data(verbatimLongitude=row.verbatimLongitude, verbatimLatitude=row.verbatimLatitude, coordinatePrecision=coordinatePrecision)
            df.loc[i,'standardRawLongitude'] = grid_data.get('standardRawLon')
            df.loc[i,'standardRawLatitude'] = grid_data.get('standardRawLat')
            df.loc[i,'raw_location_rpt'] = grid_data.get('raw_location_rpt')
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
            # TODO 這邊要考慮是不是本來就要完全屏蔽 不然有可能是無法轉換座標 就必須要顯示原始座標
            if grid_data.get('standardLon'):
                df.loc[i, 'verbatimLongitude'] = grid_data.get('standardLon')
            if grid_data.get('standardLat'):
                df.loc[i, 'verbatimLatitude'] = grid_data.get('standardLat')
        # 資料集
        ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
        # return tbiaDatasetID 並加上去
        return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version)
        df = df.merge(return_dataset_id)
        # 更新match_log
        # 更新資料
        df['occurrenceID'] = df['occurrenceID'].astype('str')
        existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID'])
        existed_records = get_existed_records(df['occurrenceID'].to_list(), rights_holder)
        existed_records = existed_records.replace({nan:''})
        if len(existed_records):
            df =  df.merge(existed_records,on=["occurrenceID"], how='left')
            df = df.replace({nan: None})
            # 如果已存在，取存在的tbiaID
            df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
            # 如果已存在，取存在的建立日期
            # df['created'] = df.apply(lambda x: x.created_y if x.tbiaID else now, axis=1)
            # df = df.drop(columns=['tbiaID','created_y','created_x'])
            df = df.drop(columns=['tbiaID'])
        # match_log要用更新的
        match_log = df[['occurrenceID','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','group','rightsHolder','created','modified']]
        match_log = match_log.reset_index(drop=True)
        match_log = update_match_log(match_log=match_log, now=now)
        match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{p}.csv',index=None)
        # records要用更新的
        # 已經串回原本的tbiaID，可以用tbiaID做更新
        df['is_deleted'] = False
        df = df.drop(columns=['match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','taxon_name_id','sci_index', 'datasetURL','gbifDatasetID', 'gbifID'],errors='ignore')
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
    # 成功之後 更新update_update_version 也有可能這批page 沒有資料 一樣從下一個c開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=None)
    # 更新 media rule
    for mm in media_rule_list:
        update_media_rule(media_rule=mm,rights_holder=rights_holder)


# 刪除is_deleted的records & match_log
delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))

# 打包match_log
zip_match_log(group=group,info_id=info_id)

# 更新update_version
update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)

# 更新 datahub - dataset
# update if deprecated
update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)

# update dataset info
update_dataset_info(rights_holder=rights_holder)


print('done!')