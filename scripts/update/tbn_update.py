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

# from scripts.taxon.match_tbn_utils import matching_flow
from scripts.taxon.match_utils import matching_flow
from scripts.utils import *


# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName','originalVernacularName','sourceTaxonID','scientificNameID','sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'tbri'
rights_holder = '台灣生物多樣性網絡 TBN'

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


# 自產資料 + eBird
url_list = ['https://www.tbn.org.tw/api/v25/occurrence?selfProduced=y', 'https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=4fa7b334-ce0d-4e88-aaae-2e0c138d049e']

# 從ipt上傳的tbri資料
url_list += ["https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=4410edca-3bdd-4475-98a2-de823b2266bc",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=e0b8cb67-6667-423d-ab71-08021b6485f3",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=f170f056-3f8a-4ef3-ac9f-4503cc854ce0",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=4daa291b-0e9d-4e21-b78d-6b4e96093adc",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=f3f25fcf-2930-4cf1-a495-6b31d7fa0252",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=3f9cd7e5-6d7b-40a8-8062-a18d2f2ca599",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=db09684b-0fd1-431e-b5fa-4c1532fbdb14",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=54eaea55-f346-442e-9414-039c25658877",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=617e5387-3122-47b7-b639-c9fafc35bf13",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=346c95be-c7b3-41dc-99c9-e88a18d8884a",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=f464cad8-531e-4d53-ad36-2e4430f6765e",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=cb6e756a-c56a-4dc4-bbfa-2002a0a754dd",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=cb382c4d-7b6c-40c2-9e2d-e8167380cec5",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=0528b82f-bebb-49b0-ad2e-5082ae002823",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=a1f3b9e3-60d5-49fe-a6d1-2d22a154e2b2",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=7bff8305-a1e3-4e5b-bbc3-4afe04006b88",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=3a3aae4c-5895-4ba5-b3ba-d5f7d924478d"]


now = datetime.now() + timedelta(hours=8)

if not note:
    url_index = 0
    request_url = None
else:
    # note = json.load(note)
    request_url = note.get('request_url')
    url_index = note.get('url_index')

for url in url_list[url_index:]:
    # print(url)
    # 先取得總頁數
    response = requests.get(url + '&limit=1')
    # c = 0
    if response.status_code == 200:
        result = response.json()
        total_count = result['meta']['total'] 
        total_page = math.ceil(total_count/1000) # 1182
    # print(total_count, total_page)
    for p in range(current_page,total_page,10):
        data = []
        c = p
        while c < p + 10 and c < total_page:
            # print(c, url)
            time.sleep(3)
            if not request_url:
                request_url = url
            if request_url.find('limit=1000') < 0:
                request_url += '&limit=1000'
            if request_url.find(f"apikey={os.getenv('TBN_KEY')}") < 0:
                request_url += f"&apikey={os.getenv('TBN_KEY')}"
            print(c, request_url)
            response = requests.get(request_url)
            if response.status_code == 200:
                result = response.json()
                request_url = result['links']['next']
                data += result["data"]
            c += 1
        df = pd.DataFrame(data)
        # 排除無學名或上階層資訊的欄位
        # '原始資料無物種資訊'
        df = df.replace({nan: '', None: '', 'NA': '', '-99999': '', 'N/A': ''})
        df['originalVernacularName'] = df['originalVernacularName'].replace({'原始資料無物種資訊': ''})
        # 如果 'originalVernacularName','simplifiedScientificName','vernacularName','familyScientificName' 都是空值才排除
        df = df[~((df.originalVernacularName=='')&(df.simplifiedScientificName=='')&(df.vernacularName=='')&(df.familyScientificName=='')&(df.scientificNameID==''))]
        if 'sensitiveCategory' in df.keys():
            df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]
        if 'license' in df.keys():
            df = df[(df.license!='')&(~df.license.str.contains('BY NC ND|BY-NC-ND',regex=True))]
        media_rule_list = []
        if len(df):
            df = df.reset_index(drop=True)
            df = df.replace({nan: '', None: '', 'NA': '', '-99999': '', 'N/A': ''})
            df['locality'] = df.apply(lambda x: x.county + x.municipality, axis = 1)
            df['locality'] = df['locality'].apply(lambda x: x.strip() if x else x)
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
                # 'originalVernacularName': 'originalScientificName',
                'taxonRank': 'sourceTaxonRank',
                'vernacularName': 'sourceVernacularName',
                'familyScientificName': 'sourceFamily',
                'datasetUUID': 'sourceDatasetID'
            })
            # 資料集
            df['recordType'] = df.apply(lambda x: 'col' if '標本' in x.basisOfRecord else 'occ', axis=1)
            ds_name = df[['datasetName','sourceDatasetID','datasetURL']].drop_duplicates()
            ds_name = ds_name.to_dict(orient='records')
            # return tbiaDatasetID 並加上去
            return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version)
            df = df.merge(return_dataset_id)
            df = df.drop(columns=['externalID','minimumElevationInMeters','gridID','adminareaCode',
                                'county','municipality','hour','minute','protectedStatusTW',
                                    'categoryIUCN', 'categoryRedlistTW', 'endemism', 'nativeness',
                                    'taxonGroup','scientificName','taiCOLNameCode','familyVernacularName',
                                    'datasetURL', 'datasetAuthor', 'datasetPublisher',
                                    'resourceCitationIdentifier','establishmentMeans','individualCount','partner',
                                    'identificationVerificationStatus', 'identifiedBy', 'dataSensitiveCategory',
                                    'eventID', 'samplingProtocol','source','selfProduced',
                                    'collectionID','verbatimEventDate','eventTime', 'eventPlaceAdminarea',
                                    'countyCode','tfNameCode'],errors='ignore')
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
            df['basisOfRecord'] = df['basisOfRecord'].apply(lambda x: control_basis_of_record(x))
            # dataGeneralizations 已標準化
            # 經緯度
            df['id'] = ''
            for i in df.index:
                # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
                df.loc[i,'id'] = str(bson.objectid.ObjectId())
                row = df.loc[i]
                # 如果有mediaLicense才放associatedMedia
                if 'mediaLicense' in df.keys() and 'associatedMedia' in df.keys():
                    if not row.mediaLicense:
                        df.loc[i,'associatedMedia'] = None
                    if df.loc[i, 'associatedMedia']:
                        media_rule = get_media_rule(df.loc[i, 'associatedMedia'])
                        if media_rule and media_rule not in media_rule_list:
                            media_rule_list.append(media_rule)
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
                if row.sensitiveCategory in ['縣市','座標不開放']:
                    is_hidden = True
                    df.loc[i,'dataGeneralizations'] = True
                grid_data = create_blurred_grid_data(verbatimLongitude=row.verbatimLongitude, verbatimLatitude=row.verbatimLatitude, coordinatePrecision=coordinatePrecision, is_full_hidden=is_hidden)
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
                # 要考慮是不是本來就要完全屏蔽 不然有可能是無法轉換座標 就必須要顯示原始座標 (從grid_data的回傳的是)
                if grid_data.get('standardLon') or is_hidden:
                    df.loc[i, 'verbatimLongitude'] = grid_data.get('standardLon')
                if grid_data.get('standardLat') or is_hidden:
                    df.loc[i, 'verbatimLatitude'] = grid_data.get('standardLat')
            # 更新match_log
            # 更新資料
            df['occurrenceID'] = df['occurrenceID'].astype('str')
            existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID'])
            existed_records = get_existed_records(df['occurrenceID'].to_list(), rights_holder)
            existed_records = existed_records.replace({nan:''})
            if len(existed_records):
                # NOTE 改成只用occurrenceID對應
                df = df.merge(existed_records,on="occurrenceID", how='left')
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
            match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{url_index}_{p}.csv',index=None)
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
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=json.dumps({'url_index': url_index, 'request_url': request_url}))
        # 更新 media rule
        for mm in media_rule_list:
            update_media_rule(media_rule=mm,rights_holder=rights_holder)
    url_index += 1
    current_page = 0 # 換成新的url時要重新開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=0, note=json.dumps({'url_index': url_index, 'request_url': None}))


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