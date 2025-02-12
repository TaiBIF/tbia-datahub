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

from scripts.taxon.match_utils import matching_flow_new
from scripts.utils import *


# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'nps'
rights_holder = '臺灣國家公園生物多樣性資料庫'

# 在portal.Partner.info裡面的id
info_id = 0

# 排除重複資料集
duplicated_dataset_list = ['臺灣林業試驗所植物標本館','國立臺灣博物館典藏數位化計畫']

locality_map = {
    'YMS': '陽明山國家公園',
    'TRK': '太魯閣國家公園',
    'SP': '雪霸國家公園',
    'ES': '玉山國家公園',
    'KT': '墾丁國家公園',
    'KM': '金門國家公園',
    'DS': '東沙環礁國家公園',
    'TJ': '台江國家公園',
    'SNNP': '壽山國家自然公園',
    'SSNP': '壽山國家自然公園',
    'SPM': '澎湖南方四島國家公園',
    'TCMP': '台中都會公園',
    'KCMP': '高雄都會公園',
}


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


url = f"https://npgis.cpami.gov.tw//TBiAOpenApi/api/Data/Get?Token={os.getenv('CPAMI_KEY')}"
response = requests.get(url)
if response.status_code == 200:
    result = response.json()
    total_page = result['Meta']['TotalPages'] # 2611 

now = datetime.now() + timedelta(hours=8)

for p in range(current_page,total_page,10):
# for p in [0]:
    print(p)
    data = []
    c = p
    while c < p + 10 and c < total_page:
        c+=1
        print('page:',c)
        # time.sleep(60)
        url = f"https://npgis.cpami.gov.tw//TBiAOpenApi/api/Data/Get?Token={os.getenv('CPAMI_KEY')}&Page={c}"
        response = requests.get(url)
        if response.status_code == 200:
            result = response.json()
            data += result.get('Data')
    df = pd.DataFrame(data)
    # 如果 'sourceScientificName','sourceVernacularName' 都是空值才排除
    df = df.replace({nan: '', None: '', 'NA': '', '-99999': '', 'N/A': ''})
    df = df.rename(columns={'basicOfRecord': 'basisOfRecord', 'created': 'sourceCreated', 'modified': 'sourceModified', 
                            'scientificName': 'sourceScientificName', 'isPreferredName': 'sourceVernacularName', 'taxonRank': 'sourceTaxonRank'})
    df = df[~((df.sourceScientificName=='')&(df.sourceVernacularName==''))]
    # df = df[~(df.isPreferredName.isin([nan,'',None])&df.scientificName.isin([nan,'',None]))]
    if 'sensitiveCategory' in df.keys():
        df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]
    # 排除重複資料集
    df = df[~df.datasetName.isin(duplicated_dataset_list)]
    if 'license' in df.keys():
        df = df[(df.license!='')&(~df.license.str.contains('ND|nd',regex=True))]
    else:
        df = []
    media_rule_list = []
    if len(df):
        df = df.reset_index(drop=True)
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
        df['sourceCreated'] = df['sourceCreated'].apply(lambda x: convert_date(x))
        df['sourceModified'] = df['sourceModified'].apply(lambda x: convert_date(x))
        df['group'] = group
        df['rightsHolder'] = rights_holder
        df['created'] = now
        df['modified'] = now
        # 地點
        df['locality'] = df['locality'].apply(lambda x: locality_map[x] if x in locality_map.keys() else x)
        df['locality'] = df['locality'].apply(lambda x: x.strip() if x else x)
        # 數量 
        df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
        # basisOfRecord
        df['recordType'] = df.apply(lambda x: 'col' if '標本' in x.basisOfRecord else 'occ', axis=1)
        df['basisOfRecord'] = df['basisOfRecord'].apply(lambda x: control_basis_of_record(x))
        # dataGeneralizations 已標準化
        df['id'] = ''
        df = df.replace({nan:None})
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
            # TODO 目前都還沒有給未模糊化資料
            # 座標模糊化
            try:
                coordinatePrecision = float(row.coordinatePrecision)
                df.loc[i,'dataGeneralizations'] = True
            except:
                coordinatePrecision = None
            is_hidden = False # 座標是否完全屏蔽
            if not coordinatePrecision and row.sensitiveCategory == '輕度':
                coordinatePrecision = 0.01
                df.loc[i,'dataGeneralizations'] = True
            elif not coordinatePrecision and row.sensitiveCategory == '重度':
                coordinatePrecision = 0.1
                df.loc[i,'dataGeneralizations'] = True
            if row.sensitiveCategory in ['縣市','座標不開放','分類群不開放']:
                is_hidden = True
                df.loc[i,'dataGeneralizations'] = True
            grid_data = create_blurred_grid_data(verbatimLongitude=row.verbatimLongitude, verbatimLatitude=row.verbatimLatitude, coordinatePrecision=coordinatePrecision, is_full_hidden=is_hidden)
            county, municipality = return_town(grid_data)
            if row.sensitiveCategory in ['縣市','座標不開放','分類群不開放']:
                df.loc[i,'rawCounty'] = county
                df.loc[i,'rawMunicipality'] = municipality
            else:
                df.loc[i,'county'] = county
                df.loc[i,'municipality'] = municipality
            df.loc[i,'standardRawLongitude'] = grid_data.get('standardRawLon') if df.loc[i,'dataGeneralizations'] else None
            df.loc[i,'standardRawLatitude'] = grid_data.get('standardRawLat') if df.loc[i,'dataGeneralizations'] else None
            df.loc[i,'raw_location_rpt'] = grid_data.get('raw_location_rpt') if df.loc[i,'dataGeneralizations'] else None
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
            # 這邊要考慮是不是本來就要完全屏蔽 不然有可能是無法轉換座標 就必須要顯示原始座標
            if grid_data.get('standardLon') or is_hidden:
                df.loc[i, 'verbatimLongitude'] = grid_data.get('standardLon')
            if grid_data.get('standardLat') or is_hidden:
                df.loc[i, 'verbatimLatitude'] = grid_data.get('standardLat')
            # 日期
            df.loc[i, ['eventDate','standardDate','year','month','day']] = convert_year_month_day(row)
        for d_col in ['year','month','day']:
            if d_col in df.keys():
                df[d_col] = df[d_col].fillna(0).astype(int).replace({0: None})
        df = df.replace({nan: None})
        df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
        # 資料集
        ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
        # return tbiaDatasetID 並加上去
        return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version)
        df = df.merge(return_dataset_id)
        # 更新match_log
        # 更新資料
        df['occurrenceID'] = df['occurrenceID'].astype('str')
        if 'catalogNumber' not in df.keys():
            df['catalogNumber'] = ''
        else:
            df['catalogNumber'] = df['catalogNumber'].astype('str')
        # existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID'])
        # existed_records = get_existed_records(df['occurrenceID'].to_list(), rights_holder)
        # existed_records = existed_records.replace({nan:''})
        existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])
        existed_records = get_existed_records(occ_ids=df[df.occurrenceID!='']['occurrenceID'].to_list(), rights_holder=rights_holder, cata_ids=df[df.catalogNumber!='']['catalogNumber'].to_list())
        existed_records = existed_records.replace({nan:''})
        if len(existed_records):
            # df =  df.merge(existed_records,on=["occurrenceID"], how='left')
            df = df.merge(existed_records, how='left')
            df = df.replace({nan: None})
            # 如果已存在，取存在的tbiaID
            df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
            # 如果已存在，取存在的建立日期
            # df['created'] = df.apply(lambda x: x.created_y if x.tbiaID else now, axis=1)
            # df = df.drop(columns=['tbiaID','created_y','created_x'])
            df = df.drop(columns=['tbiaID'])
        # match_log要用更新的
        match_log = df[['occurrenceID','catalogNumber','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','group','rightsHolder','created','modified']]
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
        for l in range(0, len(df), 1000):
            df[l:l+1000].to_sql('records', db, # schema='my_schema',
                    if_exists='append',
                    index=False,
                    method=records_upsert)
        # 成功之後 更新update_update_version
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=None)
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
# update_dataset_info(rights_holder=rights_holder)


print('done!')