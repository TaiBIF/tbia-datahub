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

from scripts.taxon.match_utils import matching_flow
from scripts.utils import *

# 比對學名時使用的欄位
sci_cols = ['taxonID','sourceVernacularName', 'sourceScientificName','originalVernacularName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'brcas'
rights_holder = '臺灣生物多樣性資訊機構 TaiBIF'

# 在portal.Partner.info裡面的id
info_id = 0

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


if not note:
    d_list_index = 0
    # request_url = None
    dataset_list = []
else:
    # note = json.load(note)
    d_list_index = note.get('d_list_index')
    # request_url = note.get('request_url')
    dataset_list = note.get('dataset_list')



# 改用id
# partners = ['Taiwan Forestry Bureau', 
#            'Taiwan Endemic Species Research Institute',  
#             'Taiwan Forestry Research Institute',
#             'Marine National Park Headquarters', 
#             'Yushan National Park Headquarters', 
#             'National Taiwan Museum', 
#             'Water Resources Agency,Ministry of Economic Affairs']
partners = ['6ddd1cf5-0655-44ac-a572-cb581a054992', # 林保署
            '7c07cec1-2925-443c-81f1-333e4187bdea', # 生多所
            '898ba450-1627-11df-bd84-b8a03c50a862', # 林試所
            '7f2ff82e-193e-48eb-8fb5-bad64c84782a', # 國家公園
            'f40c7fe5-e64a-450c-b229-21d674ef3c28', # 國家公園
            'c57cd401-ff9e-43bd-9403-089b88a97dea', # 台博館
            'b6b89e2d-e881-41f3-bc57-213815cb9742'] # 水利署
# 排除重複資料集
# 單位間
# 改用id
# duplicated_dataset_list = ['Database of Native Plants in Taiwan',
#                             # 'Digital Archives of Taiwan Malacofauna Database',
#                             # 'ntou_db',
#                             # 'nthu_db',
#                             'The Fish Database of Taiwan',
#                             'A dataset from bottom trawl survey around Taiwan',
#                             'National Museum of Natural Science',
#                             # 'taijiang_national_park_beamtrawling_2016-2018', 確認後不排除
#                             '珊瑚健康指標之建立與保護區管理應用: 以墾丁國家公園珊瑚礁生態系為例 ',
#                             '國家公園生物多樣性資料流通(2015)',
#                             '國家公園生物資訊流通(2014)',
#                             '國家公園資料集',
#                             'national_parts_taiwan-2016',
#                             # 'National vegetation diversity inventory and mapping plan',  確認後不排除
#                             'National Taiwan Museum']
duplicated_dataset_list = [
    '36c38933-a03b-4f8b-9ba3-6987e5528179',
    '489b921b-88fe-40ca-9efc-dbb3270bfa9e',
    'ec70c946-482c-4e10-ab56-9e190c9d40f9',
    'fddbabb3-7386-4a1c-a086-f12bbabe9eb6',
    '44a761b5-5adf-4b67-adad-c5ae04637fb9',
    '06b55da4-bfb9-453d-be18-a1d1ae68ed5d',
    '836a5bd1-d440-4ebd-bb1e-0d83f91bd21a',
    'af48a08e-f523-443d-9d4d-505a01be11a4',
    '07b06590-6ecc-489e-a565-73c1f2081a02',
    '73f63477-81be-4661-8d71-003597a701c0',
    'e7b6eb08-1380-40c7-9a2e-60d2ac9b00c2',
    'c6552cda-cdb3-4711-84c1-347c6fe8ba86',
]
# 單位內
# duplicated_dataset_list += ['tad_db']
duplicated_dataset_list += ['6e54a298-6358-4994-ae50-df9a8dd4efc6']
# 取得所有台灣發布者
url = "https://portal.taibif.tw/api/v2/publisher?countryCode=TW"
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    pub = pd.DataFrame(data)
    pub = pub[~pub.publisherID.isin(partners)]

    
dataset_list = []
# 取得所有資料集
url = "https://portal.taibif.tw/api/v2/dataset"
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    dataset = pd.DataFrame(data)
    dataset = dataset[dataset.source!='GBIF']
    dataset = dataset[dataset.core.isin(['OCCURRENCE','SAMPLINGEVENT'])]
    # dataset = dataset[~dataset.publisherID.isin(pub[pub.publisherName.isin(partners)].publisherID.to_list())]
    dataset = dataset[dataset.publisherID.isin(pub.publisherID.to_list())]
    dataset = dataset[~dataset.taibifDatasetID.isin(duplicated_dataset_list)]
    dataset = dataset.rename(columns={'publisherName': 'datasetPublisher', 'license': 'datasetLicense'})

if not dataset_list:
    dataset_list = dataset[['taibifDatasetID','numberOccurrence']].to_dict('tight')['data']


now = datetime.now() + timedelta(hours=8)


# d_list_index = 0

for d in dataset_list[d_list_index:]: # 20
    # test_count = 0
    total_count = d[1]
    total_page = math.ceil (total_count / 1000)
    for p in range(current_page,total_page,10):
        data = []
        c = p
        while c < p + 10 and c < total_page:
            time.sleep(1)
            offset = 1000 * c
            print(d[0], 'page:',c , ' , offset:', offset)
            # time.sleep(1)
            url = f"https://portal.taibif.tw/api/v2/occurrence/detail_occ?taibifDatasetID={d[0]}&rows=1000&offset={offset}"
            response = requests.get(url)
            if response.status_code == 200:
                result = response.json()
                data += result.get('results')
            c+=1
        # test_count += len(data)
        if len(data):
            df = pd.DataFrame(data)
            df = df.rename(columns= {
                                    'occurrenceID': 'sourceOccurrenceID',
                                    'taibifOccurrenceID': 'occurrenceID', # 使用TaiBIF給的id, 避免空值
                                    'scientificName': 'sourceScientificName',
                                    'originalScientificName': 'originalVernacularName',
                                    'taxonRank': 'sourceTaxonRank',
                                    'isPreferredName': 'sourceVernacularName',
                                    'taicolTaxonID': 'taxonID',
                                    'gbifAcceptedID': 'sourceTaxonID',
                                    'family': 'sourceFamily',
                                    'class': 'sourceClass',
                                    'order': 'sourceOrder',
                                    'decimalLatitude': 'verbatimLatitude',
                                    'decimalLongitude': 'verbatimLongitude',
                                    'taibifCreatedDate': 'sourceCreated',
                                    'taibifModifiedDate': 'sourceModified',
                                    'taibifDatasetID': 'sourceDatasetID'
                                    })
            # 如果 'sourceScientificName','sourceVernacularName', originalVernacularName, sourceClass, sourceOrder, sourceFamily 都是空值才排除
            df = df.replace({nan: '', None: '', 'NA': '', '-99999': '', 'N/A': ''})
            df = df[~((df.sourceScientificName=='')&(df.sourceVernacularName=='')&(df.originalVernacularName=='')&(df.scientificNameID=='')&(df.sourceClass=='')&(df.sourceOrder=='')&(df.sourceFamily==''))]
            if 'sensitiveCategory' in df.keys():
                df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]
            if 'license' in df.keys():
                df = df[(df.license!='')&(~df.license.str.contains('ND|nd',regex=True))]
            else:
                df = []
            # df = df[~(df.sourceVernacularName.isin([nan,'',None])&df.sourceScientificName.isin([nan,'',None]))]
            media_rule_list = []
            if len(df):
                df = df.drop(columns=['taxonGroup','taxonBackbone','kingdom','phylum','genus','geodeticDatum',
                                    'countryCode', 'country', 'county',
                                    'habitatReserve', 'wildlifeReserve', 'occurrenceStatus', 'selfProduced',
                                    'datasetShortName','establishmentMeans', 'issue'])
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
                # 數量 
                df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
                # dataGeneralizations
                df['dataGeneralizations'] = df['dataGeneralizations'].apply(lambda x: True if x else None)
                df['id'] = ''
                df['basisOfRecord'] = df['basisOfRecord'].apply(lambda x: control_basis_of_record(x))
                for i in df.index:
                    # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
                    df.loc[i,'id'] = str(bson.objectid.ObjectId())
                    row = df.loc[i]
                    # basisOfRecord 有可能是空值
                    if row.basisOfRecord:
                        if 'Specimen' in row.basisOfRecord:
                            df['recordType'] = 'col'
                        else:
                            df['recordType'] = 'occ'
                    else:
                        df['recordType'] = 'occ'
                    df.loc[i,'references'] = f"https://portal.taibif.tw/occurrence/{row.occurrenceID}" if row.occurrenceID else None
                    # 如果有mediaLicense才放associatedMedia
                    if 'mediaLicense' in df.keys() and 'associatedMedia' in df.keys():
                        if not row.mediaLicense:
                            df.loc[i,'associatedMedia'] = None  
                        if df.loc[i, 'associatedMedia']:
                            media_rule = get_media_rule(df.loc[i, 'associatedMedia'])
                            if media_rule and media_rule not in media_rule_list:
                                media_rule_list.append(media_rule)
                    grid_data = create_grid_data(verbatimLongitude=row.verbatimLongitude, verbatimLatitude=row.verbatimLatitude)
                    county, town = return_town(grid_data)
                    df.loc[i,'county'] = county
                    df.loc[i,'town'] = town
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
                    # 日期
                    df.loc[i, ['eventDate','standardDate','year','month','day']] = convert_year_month_day(row)
                for d_col in ['year','month','day']:
                    if d_col in df.keys():
                        df[d_col] = df[d_col].fillna(0).astype(int).replace({0: None})
                df = df.replace({nan: None})
                df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
                # 資料集
                df['datasetURL'] = df['sourceDatasetID'].apply(lambda x: 'https://portal.taibif.tw/dataset/' + x if x else '')
                ds_name = df[['datasetName','gbifDatasetID','sourceDatasetID','datasetURL']]
                ds_name = ds_name.merge(dataset[['taibifDatasetID','datasetPublisher','datasetLicense']], left_on='sourceDatasetID', right_on='taibifDatasetID')
                ds_name = ds_name.drop_duplicates().to_dict(orient='records')
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
                    df = df.drop(columns=['tbiaID'])
                # match_log要用更新的
                match_log = df[['occurrenceID','catalogNumber','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','group','rightsHolder','created','modified']]
                match_log = match_log.reset_index(drop=True)
                match_log = update_match_log(match_log=match_log, now=now)
                match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{d_list_index}_{p}.csv',index=None)
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
        # 成功之後 更新update_update_version
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))
    # print(test_count, total_count)
    d_list_index += 1
    current_page = 0 # 換成新的url時要重新開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=0, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))
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