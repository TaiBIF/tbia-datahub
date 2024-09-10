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
sci_cols = ['sourceScientificName', 'sourceVernacularName', 'sourceOrder', 'sourceFamily']


# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'nmns'
rights_holder = '科博典藏 (NMNS Collection)'

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
    category_index = 0
    offset = 0
else:
    category_index = note.get('category_index')
    offset = note.get('offset')


category_list = ['昆蟲學門', '兩棲爬蟲學門', '非維管束學門', '維管束學門', '真菌學門', '無脊椎動物學門']


# CollectionUrl
# ImageFocus 
# MDDATE

field_map = {
    '藏品名稱':	'sourceVernacularName',
    '目名':	'sourceOrder',
    '科名':	'sourceFamily',
    '學名':	'sourceScientificName',
    '館號/編目號':	'catalogNumber',
    # '採集地':	'locality', # 為了後面的locality_2 這邊先關起來
    '採集日':	'eventDate',
    '採集者':	'recordedBy',
    '屬名':	'genus',
    '種名':	'specificEpithet',
    '鑑定者':	'recordedBy',
    '國名':	'locality_1',
    '省/縣名':	'locality_2',
    '採集地名':	'locality_3',
    '採集日期':	'eventDate',
    '館號 TNM No.':	'catalogNumber',
    '學名 Scientific Name':	'sourceScientificName',
    '採集地 Locality':	'locality',
    '採集者 Collector':	'recordedBy',
    '採集日期 Collection Date':	'eventDate',
    '行政區域':	'locality_1',
    '採集地':	'locality_2',
    '館號 (TNM No.)':	'catalogNumber',
    '採集地 (Locality)':	'locality',
    '採集日期 (Collection Date)':	'eventDate',
    '採集者 (Collector)':	'recordedBy',
    '保存方式':	'preservation',
    '數量':	'organismQuantity'
}

field_list = list(set(field_map.values()))


now = datetime.now() + timedelta(hours=8) 


for now_category in category_list[category_index:]:
    has_more_data = True
# for p in range(current_page,total_page,10):
    # print(p)
    data = []
    # c = p
    all_count = 0
    while has_more_data:
        url = 'https://collections.culture.tw/getMetadataList.aspx?FORMAT=NMNS&DEPARTMENT={}&LIMIT=100&OFFSET={}'.format(now_category, offset)
        resp = requests.get(url)
        try:
            resp = resp.json()
        except:
            resp = []
        all_count += len(resp)
        if len(resp) < 100:
            has_more_data = False
            offset = 0 
        else:
            offset += 100
        for r in resp:
            now_dict = {'sourceModified': r.get('MDDATE'), 
                        'references': r.get('CollectionUrl'),
                        'associatedMedia': r.get('ImageFocus'),
                        'license': r.get('GalCC')}
            for rr in r.get('MetaData_NMNS'):
                if rr.get('Caption') in field_map.keys():
                    now_dict[field_map[rr.get('Caption')]] = rr.get('Value')
            data.append(now_dict)
        if len(data) > 9999 or not has_more_data:
            print(now_category, len(data), all_count)
            df = pd.DataFrame(data)
            data = [] # 重新下一個loop
            for fl in field_list:
                if fl not in df.keys():
                    df[fl] = ''
            df = df.replace({nan: '', None: '', 'NA': '', '-99999': '', 'N/A': ''})
            # 如果有資料沒有這些欄位 要先幫忙補上去
            for sci_key in ['sourceScientificName', 'sourceVernacularName', 'sourceOrder', 'sourceFamily']:
                if sci_key not in df.keys():
                    df[sci_key] = ''
            if now_category == '真菌學門':
                df['sourceVernacularName'] = ''
            if 'genus' in df.keys() and 'specificEpithet' in df.keys():
                df['sourceScientificName'] = df.apply(lambda x: x.genus + ' ' + x.specificEpithet ,axis=1)
            df = df[~((df.sourceScientificName=='')&(df.sourceVernacularName=='')&(df.sourceOrder=='')&(df.sourceFamily==''))]
            media_rule_list = []
            if len(df):
                df = df.replace({nan: '', None: '', 'NA': '', '-99999': '', 'N/A': ''})
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
                # df['sourceCreated'] = df['sourceCreated'].apply(lambda x: convert_date(x))
                df['datasetName'] = rights_holder + '-' + now_category
                df['sourceModified'] = df['sourceModified'].apply(lambda x: convert_date(x))
                df['group'] = group
                df['rightsHolder'] = rights_holder
                df['created'] = now
                df['modified'] = now
                df['recordType'] = 'col'
                # 日期
                if 'event' in df.keys():
                    df['standardDate'] = df['eventDate'].apply(lambda x: convert_date(x))
                # 數量 
                if 'organismQuantity' in df.keys():
                    df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
                # basisOfRecord 無資料
                # 敏感層級 無資料
                df['id'] = ''
                for i in df.index:
                    # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
                    df.loc[i,'id'] = str(bson.objectid.ObjectId())
                    row = df.loc[i]
                    # 在這邊處理出現地的問題
                    locality_list = []
                    for locality_ in ['locality', 'locality_1', 'locality_2', 'locality_3']:
                        if row.get(locality_):
                            locality_list.append(row.get(locality_).strip())
                    df.loc[i, 'locality'] = ' '.join(locality_list)
                    if 'associatedMedia' in df.keys():
                        # if not row.mediaLicense:
                        #     df.loc[i,'associatedMedia'] = None
                        if df.loc[i, 'associatedMedia']:
                            df.loc[i,'mediaLicense'] = 'OGDL' # 幫忙補上mediaLicense
                            media_rule = get_media_rule(df.loc[i, 'associatedMedia'])
                            if media_rule and media_rule not in media_rule_list:
                                media_rule_list.append(media_rule)
                    # 目前沒有經緯度資料
                # 資料集
                ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
                # return tbiaDatasetID 並加上去
                return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version)
                df = df.merge(return_dataset_id)
                # 更新match_log
                # 更新資料
                df['occurrenceID'] = df['catalogNumber']
                df['occurrenceID'] = df['occurrenceID'].astype('str')
                existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID'])
                existed_records = get_existed_records(df['occurrenceID'].to_list(), rights_holder)
                existed_records = existed_records.replace({nan:''})
                if len(existed_records):
                    df = df.merge(existed_records,on=["occurrenceID"], how='left')
                    df = df.replace({nan: None})
                    # 如果已存在，取存在的tbiaID
                    df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
                    df = df.drop(columns=['tbiaID'])
                # match_log要用更新的
                match_log = df[['occurrenceID','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','group','rightsHolder','created','modified']]
                match_log = match_log.reset_index(drop=True)
                match_log = update_match_log(match_log=match_log, now=now)
                match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{now_category}.csv',index=None)
                # records要用更新的
                # 已經串回原本的tbiaID，可以用tbiaID做更新
                df['is_deleted'] = False
                df = df.drop(columns=['match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','taxon_name_id','sci_index', 'datasetURL','gbifDatasetID', 'gbifID', 'locality_1', 'locality_2', 'locality_3'],errors='ignore')        # 最後再一起匯出
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
            update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=None, note=json.dumps({'catogory_index': category_index, 'offset': offset}))
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