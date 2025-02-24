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
sci_cols = ['taxonID','sourceScientificName','sourceVernacularName','originalVernacularName','sourceTaxonID','sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 


# 單位資訊
group = 'tbri'
rights_holder = '台灣生物多樣性網絡 TBN'

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
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=3a3aae4c-5895-4ba5-b3ba-d5f7d924478d",
        "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=6ef6360c-c904-4eab-87fe-7bd234cb5c42",
        ]


# 取得dataset info
dataset_url = 'https://www.tbn.org.tw/api/v25/dataset?modified=1900-01-01'
dataset_array = []
has_more_dataset = True

while has_more_dataset:
    print(dataset_url)
    resp = requests.get(dataset_url)
    if resp.status_code == 200:
        resp = resp.json()
        dataset_array += resp.get('data')
        if resp['links'].get('next'):
            dataset_url = resp['links'].get('next')
        else: 
            has_more_dataset = False
    else:
        has_more_dataset = False

dataset = pd.DataFrame(dataset_array)


now = datetime.now() + timedelta(hours=8)

if not note:
    url_index = 0
    request_url = None
else:
    request_url = note.get('request_url')
    url_index = note.get('url_index')



for url in url_list[url_index:]:
    # 先取得總頁數
    response = requests.get(url + '&limit=1')
    if response.status_code == 200:
        result = response.json()
        total_count = result['meta']['total'] 
        total_page = math.ceil(total_count/1000) # 1182
    for p in range(current_page,total_page,10):
        data = []
        c = p
        while c < p + 10 and c < total_page:
            time.sleep(0.5)
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
        # 如果學名相關的欄位都是空值才排除
        df = df.replace(to_quote_dict)
        df['originalVernacularName'] = df['originalVernacularName'].replace({'原始資料無物種資訊': ''})
        df = df[~((df.originalVernacularName=='')&(df.simplifiedScientificName=='')&(df.vernacularName=='')&(df.familyScientificName=='')&(df.taiCOLTaxonID==''))]
        if 'sensitiveCategory' in df.keys():
            df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]
        if 'license' in df.keys():
            df = df[(df.license!='')&(~df.license.str.contains('ND|nd',regex=True))]
        media_rule_list = []
        if len(df):
            df = df.reset_index(drop=True)
            df = df.replace(to_quote_dict)
            # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
            df['id'] = df.apply(lambda x: str(bson.objectid.ObjectId()), axis=1)
            df['locality'] = df.apply(lambda x: x.county + x.municipality, axis = 1)
            df['locality'] = df['locality'].apply(lambda x: x.strip() if x else x)
            # 若沒有individualCount 則用organismQuantity 
            df['organismQuantity'] = df.apply(lambda x: x.individualCount if x.individualCount else x.organismQuantity, axis = 1)
            df = df.rename(columns={
                'created': 'sourceCreated',
                'modified': 'sourceModified',
                'simplifiedScientificName': 'sourceScientificName',
                'decimalLatitude': 'verbatimLatitude', 
                'decimalLongitude': 'verbatimLongitude',
                'geodeticDatum': 'verbatimSRS',
                'taiCOLTaxonID': 'taxonID',
                'taxonUUID': 'sourceTaxonID',
                'taxonRank': 'sourceTaxonRank',
                'vernacularName': 'sourceVernacularName',
                'familyScientificName': 'sourceFamily',
                'datasetUUID': 'sourceDatasetID'
            })
            for col in cols_str_ends:
                if col in df.keys():
                    df[col] = df[col].apply(check_id_str_ends)
            df = df.drop(columns=['externalID','minimumElevationInMeters','gridID','adminareaCode',
                                'county','municipality','hour','minute','protectedStatusTW',
                                    'categoryIUCN', 'categoryRedlistTW', 'endemism', 'nativeness',
                                    'taxonGroup','scientificName','taiCOLNameCode','familyVernacularName',
                                    'datasetURL', 'datasetAuthor', 'datasetPublisher',
                                    'resourceCitationIdentifier','establishmentMeans','individualCount','partner',
                                    'identificationVerificationStatus', 'identifiedBy', 'dataSensitiveCategory',
                                    'eventID', 'samplingProtocol','source','selfProduced',
                                    'collectionID','verbatimEventDate','eventTime', 'eventPlaceAdminarea',
                                    'countyCode','tfNameCode', 'scientificNameID'],errors='ignore')
            df['taxonID'] = df['taxonID'].apply(lambda x: x if len(str(x)) == 8 else '')
            sci_names = df[sci_cols].drop_duplicates().reset_index(drop=True)
            now_s = time.time()
            sci_names = matching_flow_new(sci_names)
            print('match', time.time()-now_s)
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
            # 數量
            df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
            # basisOfRecord
            df['basisOfRecord'] = df['basisOfRecord'].apply(lambda x: control_basis_of_record(x))
            df['recordType'] = df.apply(lambda x: 'col' if 'Specimen' in x.basisOfRecord else 'occ', axis=1)
            #  如果有mediaLicense才放associatedMedia
            if 'mediaLicense' in df.keys() and 'associatedMedia' in df.keys():
                df['associatedMedia'] = df['associatedMedia'].replace({None: '', np.nan: ''})
                df['associatedMedia'] = df.apply(lambda x: x.associatedMedia if x.mediaLicense else '', axis=1)
            df['media_rule_list'] = df[df.associatedMedia.notnull()]['associatedMedia'].apply(lambda x: get_media_rule(x))
            media_rule_list += list(df[df.media_rule_list.notnull()].media_rule_list.unique())
            # dataGeneralizations 已標準化
            # 座標模糊化
            now_s = time.time()
            # 地理資訊
            df['coordinatePrecision'] = df.apply(lambda x: coor_precision(x), axis=1)
            df['coordinatePrecision'] = df['coordinatePrecision'].replace({np.nan: None})
            df['is_hidden'] = df.apply(lambda x: True if x.sensitiveCategory in ['縣市','座標不開放'] else False, axis=1)
            for g in geo_keys:
                if g not in df.keys():
                    df[g] = ''
            df[geo_keys] = df.apply(lambda x: pd.Series(create_blurred_grid_data_new(x.verbatimLongitude, x.verbatimLatitude, x.coordinatePrecision, x.dataGeneralizations, is_full_hidden=x.is_hidden)),  axis=1)
            print('coor', time.time()-now_s)
            now_s = time.time()
            # 待處理
            print(time.time()-now_s)
            # 年月日
            df[date_keys] = df.apply(lambda x: pd.Series(convert_year_month_day_new(x.to_dict())), axis=1)
            for d_col in ['year','month','day']:
                if d_col in df.keys():
                    df[d_col] = df[d_col].fillna(0).astype(int).replace({0: None})
            df = df.replace(to_none_dict)
            df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
            # 資料集
            ds_name = df[['datasetName','sourceDatasetID','datasetURL','datasetPublisher']].drop_duplicates()
            ds_name = ds_name.merge(dataset[['datasetUUID','datasetLicense']], left_on='sourceDatasetID', right_on='datasetUUID')
            ds_name = ds_name.drop_duplicates().to_dict(orient='records')
            # return tbiaDatasetID 並加上去
            return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version)
            df = df.merge(return_dataset_id)
            # 取得已建立的tbiaID
            df['occurrenceID'] = df['occurrenceID'].astype('str')
            if 'catalogNumber' not in df.keys():
                df['catalogNumber'] = ''
            else:
                df['catalogNumber'] = df['catalogNumber'].astype('str')
            now_s = time.time()
            existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])
            existed_records = get_existed_records(occ_ids=df[df.occurrenceID!='']['occurrenceID'].to_list(), rights_holder=rights_holder, cata_ids=df[df.catalogNumber!='']['catalogNumber'].to_list())
            existed_records = existed_records.replace({nan:''})
            if len(existed_records):
                df = df.merge(existed_records, how='left')
                df = df.replace(to_none_dict)
                # 如果已存在，取存在的tbiaID
                df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
                df = df.drop(columns=['tbiaID'])
            print('get_existed', time.time()-now_s)
            # 更新match_log
            now_s = time.time()
            match_log = df[match_log_cols]
            match_log = match_log.reset_index(drop=True)
            match_log = update_match_log(match_log=match_log, now=now)
            match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{url_index}_{p}.csv',index=None)
            print('matchlog', time.time()-now_s)
            # 用tbiaID更新records
            df['is_deleted'] = False
            df['update_version'] = int(update_version)
            df = df.rename(columns=({'id': 'tbiaID'}))
            df = df.drop(columns=[ck for ck in df.keys() if ck not in records_cols],errors='ignore')
            now_s = time.time()
            for l in range(0, len(df), 1000):
                df[l:l+1000].to_sql('records', db,
                        if_exists='append',
                        index=False,
                        method=records_upsert)
            print('tosql', time.time()-now_s)
            # 更新 media rule
            for mm in media_rule_list:
                update_media_rule(media_rule=mm,rights_holder=rights_holder)
        # 成功之後 更新update_update_version 也有可能這批page 沒有資料 一樣從下一個c開始
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=json.dumps({'url_index': url_index, 'request_url': request_url}))
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


print('done!')