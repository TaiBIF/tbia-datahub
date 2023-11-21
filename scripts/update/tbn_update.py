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

from scripts.taxon.match_tbn_utils import matching_flow
from scripts.utils import *


# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName','originalScientificName','sourceTaxonID','scientificNameID','sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'tbri'
rights_holder = '台灣生物多樣性網絡 TBN'

# 在portal.Partner.info裡面的id
info_id = 0

# 先將records設為is_deleted='t'
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


# 自產資料 + eBird
url_list = ['https://www.tbn.org.tw/api/v25/occurrence?selfProduced=y', 'https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=4fa7b334-ce0d-4e88-aaae-2e0c138d049e']

now = datetime.now() + timedelta(hours=8)

url_index = 0
for url in url_list:
    url_index += 1
    response = requests.get(url + '&limit=1')
    c = 0
    if response.status_code == 200:
        result = response.json()
        total_count = result['meta']['total'] 
        total_page = math.ceil(total_count/1000) # 1182
    # print(total_count, total_page)
    for p in range(0,total_page,10):
        data = []
        c = p
        while c < p + 10 and c < total_page:
            # print(c, url)
            time.sleep(1)
            if url.find('limit=1000') < 0:
                url += '&limit=1000'
            if url.find(f"apikey={os.getenv('TBN_KEY')}") < 0:
                url += f"&apikey={os.getenv('TBN_KEY')}"
            # if f"&apikey={os.getenv('TBN_KEY')}&limit=1000" not in url:
            #     url = url + f"&apikey={os.getenv('TBN_KEY')}&limit=1000"
            print(c, url)
            response = requests.get(url)
            if response.status_code == 200:
                result = response.json()
                url = result['links']['next']
                data += result["data"]
            c += 1
        df = pd.DataFrame(data)
        df = df[~(df.originalVernacularName.isin([nan,'',None])&df.scientificName.isin([nan,'',None]))]
        if len(df):
            df = df.reset_index(drop=True)
            df = df.replace({nan: '', None: ''})
            df['locality'] = df.apply(lambda x: x.county + x.municipality, axis = 1)
            # 若沒有individualCount 則用organismQuantity 
            df['organismQuantity'] = df.apply(lambda x: x.individualCount if x.individualCount else x.organismQuantity, axis = 1)
            # df.apply(lambda x: f'{x.year}-{x.month:02}-{x.day:02} {x.hour:02}:{x.minute:02}', axis = 1)
            df = df.rename(columns={
                'created': 'sourceCreated',
                'modified': 'sourceModified',
                'simplifiedScientificName': 'sourceScientificName',
                'decimalLatitude': 'verbatimLatitude', 
                'decimalLongitude': 'verbatimLongitude',
                'geodeticDatum': 'verbatimSRS',
                # 'year': 'sourceYear',
                # 'month': 'sourceMonth',
                # 'day': 'sourceDay',
                'taxonUUID': 'sourceTaxonID',
                'originalVernacularName': 'originalScientificName',
                'taxonRank': 'sourceTaxonRank',
                'vernacularName': 'sourceVernacularName',
                'familyScientificName': 'sourceFamily',
            })
            df = df.drop(columns=['externalID','minimumElevationInMeters','gridID','adminareaCode',
                                'county','municipality','hour','minute','protectedStatusTW',
                                    'categoryIUCN', 'categoryRedlistTW', 'endemism', 'nativeness',
                                    'taxonGroup','scientificName','taiCOLNameCode','familyVernacularName',
                                    'datasetUUID','datasetURL', 'datasetAuthor', 'datasetPublisher',
                                    'resourceCitationIdentifier','establishmentMeans','individualCount','partner',
                                    'identificationVerificationStatus', 'identifiedBy', 'dataSensitiveCategory',
                                    'eventID', 'samplingProtocol','source','selfProduced',
                                    'collectionID','verbatimEventDate','eventTime', 'eventPlaceAdminarea',
                                    'countyCode','tfNameCode'],errors='ignore')
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
            df['references'] = df.apply(lambda x: f"https://www.tbn.org.tw/occurrence/{x.occurrenceID}" if x.occurrenceID else None, axis=1)
            df['sourceModified'] = df['sourceModified'].apply(lambda x: convert_date(x))
            df['sourceCreated'] = df['sourceCreated'].apply(lambda x: convert_date(x))
            df['group'] = group
            df['rightsHolder'] = rights_holder
            df['created'] = now
            df['modified'] = now
            # 日期
            df['standardDate'] = df['eventDate'].apply(lambda x: convert_date(x))
            # 數量
            df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
            # basisOfRecord
            df['recordType'] = df.apply(lambda x: 'col' if '標本' in x.basisOfRecord else 'occ', axis=1)
            df['basisOfRecord'] = df['basisOfRecord'].apply(lambda x: control_basis_of_record(x))
            # dataGeneralizations 已標準化
            # 經緯度
            df['grid_1'] = '-1_-1'
            df['grid_5'] = '-1_-1'
            df['grid_10'] = '-1_-1'
            df['grid_100'] = '-1_-1'
            df['id'] = ''
            df['standardLongitude'] = None
            df['standardLatitude'] = None
            df['location_rpt'] = None
            for i in df.index:
                # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
                df.loc[i,'id'] = str(bson.objectid.ObjectId())
                row = df.loc[i]
                # 座標模糊化
                try:
                    coordinatePrecision = float(row.coordinatePrecision)
                except:
                    coordinatePrecision = None
                if (row.dataGeneralizations and coordinatePrecision) or row.sensitiveCategory in ['輕度','重度','縣市','座標不開放','物種不開放']:
                    fuzzy_lon = None
                    fuzzy_lat = None
                    standardRawLon, standardRawLat, raw_location_rpt = standardize_coor(row.verbatimLongitude, row.verbatimLatitude)
                    if standardRawLon and standardRawLat:
                        df.loc[i, 'verbatimRawLatitude'] = float(row.verbatimLatitude)
                        df.loc[i, 'verbatimRawLongitude'] = float(row.verbatimLongitude)
                        df.loc[i, 'raw_location_rpt'] = raw_location_rpt
                        df.loc[i, 'standardRawLongitude'] = standardRawLon
                        df.loc[i, 'standardRawLatitude'] = standardRawLat
                    if not coordinatePrecision and row.sensitiveCategory == '輕度':
                        coordinatePrecision = 0.01
                    elif not coordinatePrecision and row.sensitiveCategory == '重度':
                        coordinatePrecision = 0.1
                    if coordinatePrecision and row.sensitiveCategory not in ['縣市','座標不開放','物種不開放']:
                        ten_times = math.pow(10, len(str(coordinatePrecision).split('.')[-1]))
                        if row.verbatimLongitude:
                            fuzzy_lon = math.floor(float(row.verbatimLongitude)*ten_times)/ten_times
                        if row.verbatimLatitude:
                            fuzzy_lat = math.floor(float(row.verbatimLatitude)*ten_times)/ten_times
                        df.loc[i, 'coordinatePrecision'] = coordinatePrecision
                    # elif row.sensitiveCategory in ['縣市','座標不開放','物種不開放']:
                    #     fuzzy_lon = None
                    #     fuzzy_lat = None
                    df.loc[i, 'verbatimLongitude'] = fuzzy_lon
                    df.loc[i, 'verbatimLatitude'] = fuzzy_lat  
                    row = df.loc[i]
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
            existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID'])
            existed_records = get_existed_records(df['occurrenceID'].to_list(), rights_holder)
            # with db.begin() as conn:
            #     qry = sa.text("""select "tbiaID", "occurrenceID", "created" from records  
            #                     where "rightsHolder" = '{}' AND "occurrenceID" IN {}  """.format(rights_holder, str(df.occurrenceID.to_list()).replace('[','(').replace(']',')')) )
            #     resultset = conn.execute(qry)
            #     results = resultset.mappings().all()
            #     existed_records = pd.DataFrame(results)
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
            match_log = df[['occurrenceID','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group','rightsHolder','created','modified']]
            match_log = match_log.reset_index(drop=True)
            match_log = update_match_log(match_log=match_log, now=now)
            match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{url_index}_{p}.csv',index=None)
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


# 刪除is_deleted的records & match_log
delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))

# 打包match_log
zip_match_log(group=group,info_id=info_id)

print('done!')