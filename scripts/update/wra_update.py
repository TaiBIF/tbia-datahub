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
sci_cols = ['taxonID','sourceVernacularName', 'sourceScientificName','originalVernacularName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 


# 單位資訊
group = 'wra'
rights_holder = '河川環境資料庫'

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
    dataset_list = []
else:
    d_list_index = note.get('d_list_index')
    dataset_list = note.get('dataset_list')


# 取得水利署發布資料集
publisher_id = 'b6b89e2d-e881-41f3-bc57-213815cb9742'
url = f"https://portal.taibif.tw/api/v3/dataset?publisherID={publisher_id}"
response = requests.get(url)
if response.status_code == 200:
    data = response.json()['data']
    dataset = pd.DataFrame(data)
    dataset = dataset.rename(columns={'publisherName': 'datasetPublisher', 'license': 'datasetLicense'})


if not dataset_list:
    dataset_list = [[d['taibifDatasetID'],d['numberOccurrence']] for d in data if d['core'] in (['OCCURRENCE','SAMPLINGEVENT'])]

now = datetime.now() + timedelta(hours=8)


for d in dataset_list[d_list_index:]:
    total_count = d[1]
    total_page = math.ceil (total_count / 1000)
    for p in range(current_page,total_page,10):
        data = []
        c = p
        while c < p + 10 and c < total_page:
            offset = 1000 * c
            print('page:',c , ' , offset:', offset)
            url = f"https://portal.taibif.tw/api/v3/occurrence?taibifDatasetID={d[0]}&rows=1000&offset={offset}"
            response = requests.get(url)
            if response.status_code == 200:
                result = response.json()
                data += result.get('data')
            c+=1
        if len(data):
            df = pd.DataFrame(data)
            df = df.replace(to_quote_dict)
            df = df.rename(columns= {'originalOccurrenceID': 'occurrenceID',
                                    'taibifOccurrenceID': 'sourceOccurrenceID',
                                    'taibifScientificName': 'sourceScientificName',
                                    'originalScientificName': 'originalVernacularName',
                                    'taxonRank': 'sourceTaxonRank',
                                    'vernacularName': 'sourceVernacularName',
                                    'family': 'sourceFamily',
                                    'class': 'sourceClass',
                                    'order': 'sourceOrder',
                                    'decimalLatitude': 'verbatimLatitude',
                                    'decimalLongitude': 'verbatimLongitude',
                                    'taibifModifiedDate': 'sourceModified',
                                    'taibifDatasetID': 'sourceDatasetID' })
            # 如果 'taxonBackbone' == 'TaiCOL' 給予taxonID
            df['taxonID'] = df.apply(lambda x: x.scientificNameID if x.taxonBackbone == 'TaiCOL' else None ,axis=1)
            # 如果學名相關的欄位都是空值才排除
            df = df[~((df.sourceScientificName=='')&(df.sourceVernacularName=='')&(df.originalVernacularName=='')&(df.sourceClass=='')&(df.sourceOrder=='')&(df.sourceFamily=='')&(df.scientificNameID==''))]
            if 'sensitiveCategory' in df.keys():
                df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]
            if 'license' in df.keys():
                df = df[(df.license!='')&(~df.license.str.contains('ND|nd',regex=True))]
            else:
                df = []
            media_rule_list = []
            if len(df):
                df = df.reset_index(drop=True)
                df = df.replace(to_quote_dict)
                # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
                df['id'] = df.apply(lambda x: str(bson.objectid.ObjectId()), axis=1)
                df = df.drop(columns=['taxonGroup','taxonBackbone','kingdom','phylum','genus','geodeticDatum',
                                    'countryCode', 'country', 'county', 'habitatReserve', 'wildlifeReserve', 'occurrenceStatus',
                                    'selfProduced', 'datasetShortName','establishmentMeans', 'issue'])
                for col in cols_str_ends:
                    if col in df.keys():
                        df[col] = df[col].apply(check_id_str_ends)
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
                # basisOfRecord
                df['basisOfRecord'] = df['basisOfRecord'].apply(lambda x: control_basis_of_record(x))
                df['recordType'] = df.apply(lambda x: 'col' if 'Specimen' in x.basisOfRecord else 'occ', axis=1)
                #  如果有mediaLicense才放associatedMedia
                if 'mediaLicense' in df.keys() and 'associatedMedia' in df.keys():
                    df['associatedMedia'] = df['associatedMedia'].replace({None: '', np.nan: ''})
                    df['associatedMedia'] = df.apply(lambda x: x.associatedMedia if x.mediaLicense else '', axis=1)
                    df['media_rule_list'] = df[df.associatedMedia.notnull()]['associatedMedia'].apply(lambda x: get_media_rule(x))
                    media_rule_list += list(df[df.media_rule_list.notnull()].media_rule_list.unique())
                # 地理資訊
                for g in geo_wo_raw_keys:
                    if g not in df.keys():
                        df[g] = ''
                df[geo_wo_raw_keys] = df.apply(lambda x: pd.Series(create_grid_data_new(x.verbatimLongitude, x.verbatimLatitude)),  axis=1)
                # 年月日
                df[date_keys] = df.apply(lambda x: pd.Series(convert_year_month_day_new(x.to_dict())), axis=1)
                for d_col in ['year','month','day']:
                    if d_col in df.keys():
                        df[d_col] = df[d_col].fillna(0).astype(int).replace({0: None})
                df = df.replace(to_quote_dict)
                df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
                # 資料集
                df['datasetURL'] = df['gbifDatasetID'].apply(lambda x: 'https://www.gbif.org/dataset/' + x if x else '')
                ds_name = df[['datasetName','gbifDatasetID','sourceDatasetID','datasetURL']]
                ds_name = ds_name.merge(dataset[['taibifDatasetID','datasetPublisher','datasetLicense']], left_on='sourceDatasetID', right_on='taibifDatasetID')
                ds_name = ds_name.drop_duplicates().to_dict(orient='records')
                # return tbiaDatasetID 並加上去
                return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version, group=group)
                df = df.merge(return_dataset_id)
                # 取得已建立的tbiaID
                df['occurrenceID'] = df['occurrenceID'].astype('str')
                if 'catalogNumber' not in df.keys():
                    df['catalogNumber'] = ''
                else:
                    df['catalogNumber'] = df['catalogNumber'].astype('str')
                existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])
                existed_records = get_existed_records(occ_ids=df[df.occurrenceID!='']['occurrenceID'].to_list(), rights_holder=rights_holder, cata_ids=df[df.catalogNumber!='']['catalogNumber'].to_list(), get_reference=True)
                existed_records = existed_records.replace({nan:''})
                if len(existed_records):
                    df = df.merge(existed_records, how='left')
                    df = df.replace(to_none_dict)
                    df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
                    df = df.drop(columns=['tbiaID'])
                # 如果是新的records 再更新GBIF ID
                for i in df.index:
                    row = df.loc[i]
                    if row.gbifID:
                        df.loc[i, 'references'] = f"https://www.gbif.org/occurrence/{row.gbifID}" 
                    # 確認原本有沒有references
                    elif 'references' in existed_records.keys():
                        if not len(existed_records[(existed_records.tbiaID==row.id)&(existed_records.references!='')]):
                            gbif_id = get_gbif_id(row.gbifDatasetID, row.occurrenceID)
                            if gbif_id:
                                df.loc[i, 'references'] = f"https://www.gbif.org/occurrence/{gbif_id}"
                    else:
                        gbif_id = get_gbif_id(row.gbifDatasetID, row.occurrenceID)
                        if gbif_id:
                            df.loc[i, 'references'] = f"https://www.gbif.org/occurrence/{gbif_id}"
                df = df.replace(to_none_dict)
                # 更新match_log
                match_log = df[match_log_cols]
                match_log = match_log.reset_index(drop=True)
                match_log = update_match_log(match_log=match_log, now=now)
                match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{d_list_index}_{p}.csv',index=None)
                # 用tbiaID更新records
                df['is_deleted'] = False
                df['update_version'] = update_version
                df = df.rename(columns=({'id': 'tbiaID'}))
                df = df.drop(columns=[ck for ck in df.keys() if ck not in records_cols],errors='ignore')
                for l in range(0, len(df), 1000):
                    df[l:l+1000].to_sql('records', db,
                            if_exists='append',
                            index=False,
                            method=records_upsert)
                for mm in media_rule_list:
                    update_media_rule(media_rule=mm,rights_holder=rights_holder)
        # 成功之後 更新update_update_version
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))
    d_list_index += 1
    current_page = 0 # 換成新的url時要重新開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=0, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))


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