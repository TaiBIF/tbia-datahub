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
sci_cols = ['taxonID','sourceVernacularName', 'sourceScientificName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'brcas'
rights_holder = '臺灣生物多樣性資訊機構 TaiBIF'

# 在portal.Partner.info裡面的id
info_id = 0

# # 先將records設為is_deleted='t'
# with db.begin() as conn:
#     qry = sa.text("""update records set is_deleted = 't' where "rightsHolder" = '{}' and "group" = '{}';""".format(rights_holder, group))
#     resultset = conn.execute(qry)

response = requests.get(f'http://solr:8983/solr/tbia_records/select?fl=update_version&fq=rightsHolder:{rights_holder}&q.op=OR&q=*%3A*&rows=1&sort=update_version%20desc')
if response.status_code == 200:
    resp = response.json()
    if data := resp['response']['docs']:
        update_version = data[0]['update_version'] + 1
    else:
        update_version = 1


# 排除夥伴單位
# 改用id
# partners = ['Taiwan Forestry Bureau', 
#             'Taiwan Endemic Species Research Institute', 
#             'Taiwan Forestry Research Institute',
#             'Marine National Park Headquarters', 
#             'Yushan National Park Headquarters', 
#             'National Taiwan Museum', 
#             'Water Resources Agency,Ministry of Economic Affairs']

partners = ['6ddd1cf5-0655-44ac-a572-cb581a054992', 
            '7c07cec1-2925-443c-81f1-333e4187bdea', 
            '898ba450-1627-11df-bd84-b8a03c50a862', 
            '7f2ff82e-193e-48eb-8fb5-bad64c84782a', 
            'f40c7fe5-e64a-450c-b229-21d674ef3c28', 
            'c57cd401-ff9e-43bd-9403-089b88a97dea', 
            'b6b89e2d-e881-41f3-bc57-213815cb9742']

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
    '73f63477-81be-4661-8d71-003597a701c0'
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
    dataset_list = dataset[['taibifDatasetID','numberOccurrence']].to_dict('tight')['data']

now = datetime.now() + timedelta(hours=8)

d_list_index = 0

for d in dataset_list: # 20
    d_list_index += 1
    test_count = 0
    total_count = d[1]
    total_page = math.ceil (total_count / 1000)
    for p in range(0,total_page,10):
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
        test_count += len(data)
        if len(data):
            df = pd.DataFrame(data)
            df = df.rename(columns= {
                                    'occurrenceID': 'sourceOccurrenceID',
                                    'taibifOccurrenceID': 'occurrenceID', # 使用TaiBIF給的id, 避免空值
                                    'scientificName': 'sourceScientificName',
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
                                    })
            df = df.drop(columns=['taxonGroup','taxonBackbone','kingdom','phylum','genus','geodeticDatum',
                                'countryCode', 'country', 'county',
                                'habitatReserve', 'wildlifeReserve', 'occurrenceStatus', 'selfProduced',
                                'datasetShortName','taibifDatasetID','establishmentMeans', 'issue'])
            df = df[~(df.sourceVernacularName.isin([nan,'',None])&df.sourceScientificName.isin([nan,'',None]))]
            if len(df):
                sci_names = df[sci_cols].drop_duplicates().reset_index(drop=True)
                sci_names = matching_flow(sci_names)
                df = df.drop(columns=['taxonID'], errors='ignore')
                match_taxon_id = sci_names
                # taxon_list = list(sci_names[sci_names.taxonID!=''].taxonID.unique()) + list(sci_names[sci_names.parentTaxonID!=''].parentTaxonID.unique())
                # taxon_list = list(sci_names[sci_names.taxonID!=''].taxonID.unique())
                # final_taxon = taxon[taxon.taxonID.isin(taxon_list)]
                # final_taxon = pd.DataFrame(final_taxon)
                # if len(final_taxon):
                #     match_taxon_id = sci_names.merge(final_taxon)
                #     # 若沒有taxonID的 改以parentTaxonID串
                #     # match_parent_taxon_id = sci_names.drop(columns=['taxonID']).merge(final_taxon,left_on='parentTaxonID',right_on='taxonID')
                #     # match_parent_taxon_id['taxonID'] = ''
                #     # match_taxon_id = pd.concat([match_taxon_id, match_parent_taxon_id], ignore_index=True)
                #     # 如果都沒有對到 要再加回來
                #     match_taxon_id = pd.concat([match_taxon_id,sci_names[~sci_names.sci_index.isin(match_taxon_id.sci_index.to_list())]], ignore_index=True)
                #     match_taxon_id = match_taxon_id.replace({nan: ''})
                #     match_taxon_id[sci_cols] = match_taxon_id[sci_cols].replace({'': '-999999'})
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
                # 日期
                df['standardDate'] = df['eventDate'].apply(lambda x: convert_date(x))
                # 數量 
                df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
                # dataGeneralizations
                df['dataGeneralizations'] = df['dataGeneralizations'].apply(lambda x: True if x else None)
                # 經緯度
                df['grid_1'] = '-1_-1'
                df['grid_5'] = '-1_-1'
                df['grid_10'] = '-1_-1'
                df['grid_100'] = '-1_-1'
                df['id'] = ''
                df['standardLongitude'] = None
                df['standardLatitude'] = None
                df['location_rpt'] = None
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
                    standardLon, standardLat, location_rpt = standardize_coor(row.verbatimLongitude, row.verbatimLatitude)
                    if standardLon and standardLat:
                        df.loc[i,'standardLongitude'] = standardLon
                        df.loc[i,'standardLatitude'] = standardLat
                        df.loc[i,'location_rpt'] = location_rpt
                        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.01)
                        df.loc[i, 'grid_1'] = str(int(grid_x)) + '_' + str(int(grid_y))
                        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.05)
                        df.loc[i, 'grid_5'] = str(int(grid_x)) + '_' + str(int(grid_y))
                        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.1)
                        df.loc[i, 'grid_10'] = str(int(grid_x)) + '_' + str(int(grid_y))
                        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 1)
                        df.loc[i, 'grid_100'] = str(int(grid_x)) + '_' + str(int(grid_y))
                # 資料集
                ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
                update_dataset_key(ds_name=ds_name, rights_holder=rights_holder)
                # 更新match_log
                # 更新資料
                df['occurrenceID'] = df['occurrenceID'].astype('str')
                existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID','datasetName'])
                existed_records = get_existed_records(df['occurrenceID'].to_list(), rights_holder)
                existed_records = existed_records.replace({nan:''})
                # with db.begin() as conn:
                #     qry = sa.text("""select "tbiaID", "occurrenceID", "created" from records  
                #                     where "rightsHolder" = '{}' AND "occurrenceID" IN {}  """.format(rights_holder, str(df.occurrenceID.to_list()).replace('[','(').replace(']',')')) )
                #     resultset = conn.execute(qry)
                #     results = resultset.mappings().all()
                #     existed_records = pd.DataFrame(results)
                if len(existed_records):
                    df =  df.merge(existed_records,on=["occurrenceID","datasetName"], how='left')
                    df = df.replace({nan: None})
                    # 如果已存在，取存在的tbiaID
                    df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
                    # 如果已存在，取存在的建立日期
                    # df['created'] = df.apply(lambda x: x.created_y if x.tbiaID else now, axis=1)
                    # df = df.drop(columns=['tbiaID','created_y','created_x'])
                    df = df.drop(columns=['tbiaID'])
                # match_log要用更新的
                match_log = df[['occurrenceID','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group','rightsHolder','created','modified']]
                match_log = match_log.reset_index(drop=True)
                match_log = update_match_log(match_log=match_log, now=now)
                match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{d_list_index}_{p}.csv',index=None)
                # records要用更新的
                # 已經串回原本的tbiaID，可以用tbiaID做更新
                df['is_deleted'] = False
                df = df.drop(columns=['match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','taxon_name_id','sci_index','gbifDatasetID'],errors='ignore')
                # 最後再一起匯出
                # # 在solr裡 要使用id當作名稱 而非tbiaID
                # df.to_csv(f'/solr/csvs/updated/{group}_{info_id}_{p}.csv', index=False)
                # 存到records裏面
                df = df.rename(columns=({'id': 'tbiaID'}))
                df['update_version'] = int(update_version)
                # df = df.drop(columns=psql_records_key,errors='ignore')
                df = df.drop(columns=['gbifID'])
                df.to_sql('records', db, # schema='my_schema',
                        if_exists='append',
                        index=False,
                        method=records_upsert)
    print(test_count, total_count)


# 刪除is_deleted的records & match_log
delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))

# 打包match_log
zip_match_log(group=group,info_id=info_id)

print('done!')