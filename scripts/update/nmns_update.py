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

from scripts.taxon.match_utils import matching_flow_new_optimized, match_cols
from scripts.utils import *

records_processor = OptimizedRecordsProcessor(db, batch_size=200)
matchlog_processor = OptimizedMatchLogProcessor(db, batch_size=300)

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName', 'sourceVernacularName', 'sourceOrder', 'sourceFamily', 'sourceClass', 'sourceKingdom']


# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 


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


# 鳥獸學門 - 沒有資料
# 鳥獸學門(哺乳類蒐藏) - 沒有資料
# 鳥獸學門(鳥類蒐藏) - 沒有資料
# 非維管束學門 - 階層: 「分類資訊 Taxonomy」門 (Phylum) ， 綱 (Class) ， 目 (Order) ， 科 (Family) 逗號分隔 但有可能有前後多於空格
# 昆蟲學門 - 階層有分開欄位
# 無脊椎動物學門 - 階層有分開欄位
# 維管束學門 - 階層有分開欄位
# 真菌學門 - 階層: 「分類資訊 Taxonomy」門 (Phylum) ， 綱 (Class) ， 目 (Order) ， 科 (Family) 逗號分隔 但有可能有前後多於空格
# 兩棲爬蟲學門 - 階層有分開欄位
# 古生物學門 - 沒有資料
# 兩爬學門(魚類蒐藏) - 沒有資料



# 依據學門給kingdom

kingdom_map = {
    '昆蟲學門': 'Animalia',
    '兩棲爬蟲學門': 'Animalia',
    '無脊椎動物學門': 'Animalia',
    '維管束學門': 'Plantae',
    '真菌學門': 'Fungi',
    # '非維管束學門': None,  <-- 這裡留空，等到迴圈內依據「類別」動態處理
}

category_list = ['昆蟲學門', '兩棲爬蟲學門', '非維管束學門', '維管束學門', '真菌學門', '無脊椎動物學門']


# field_map = {
#     '中文名 Chinese Common Name': 'sourceVernacularName',
#     '藏品名稱':	'sourceVernacularName',
#     '目名':	'sourceOrder',
#     '科名':	'sourceFamily',
#     '學名':	'sourceScientificName',
#     '館號/編目號':	'catalogNumber',
#     # '採集地':	'locality', # 為了後面的locality_2 這邊先關起來
#     '採集日':	'eventDate',
#     '採集者':	'recordedBy',
#     '屬名':	'genus',
#     '種名':	'specificEpithet',
#     # '鑑定者':	'recordedBy',
#     '國名':	'locality_1',
#     '省/縣名':	'locality_2',
#     '採集地名':	'locality_3',
#     '採集日期':	'eventDate',
#     '採集號':	'recordNumber',
#     "採集號 Collector's No.":	'recordNumber',
#     '館號/編目號 Catalog No.':	'catalogNumber',
#     '館號 TNM No.':	'catalogNumber',
#     '學名 Scientific Name':	'sourceScientificName',
#     '採集地 Locality':	'locality',
#     '採集者 Collector':	'recordedBy',
#     '採集日期 Collection Date':	'eventDate',
#     '行政區域':	'locality_1',
#     '採集地':	'locality_2',
#     '館號 (TNM No.)':	'catalogNumber',
#     '採集地 (Locality)':	'locality',
#     '採集日期 (Collection Date)':	'eventDate',
#     '採集者 (Collector)':	'recordedBy',
#     '保存方式':	'preservation',
#     '數量':	'organismQuantity'
# }


field_map = {
    # --- 學名 ---
    '中文名 Chinese Common Name': 'sourceVernacularName', # 裡面可能有學名 但暫時不處理
    '學名': 'sourceScientificName',
    '學名 Scientific Name': 'sourceScientificName',
    '屬名': 'genus',
    '種名': 'specificEpithet',
    # --- 分類階層 (Kingdom/Phylum/Class 另外邏輯處理) ---
    '目名': 'sourceOrder',
    '科名': 'sourceFamily',
    # '中文科名': '', 統一用英文科名
    # --- 館藏編號 ---
    '館號/編目號 Catalog No.':  'catalogNumber',
    '館號 TNM No.': 'catalogNumber',
    '採集號':   'recordNumber',
    "採集號 Collector's No.":   'recordNumber',
    # --- 人員與時間 ---
    '採集日':   'eventDate',
    '採集日期': 'eventDate',
    '採集日期 Collection Date': 'eventDate',
    '採集者':   'recordedBy',
    '採集者 Collector': 'recordedBy',
    # '鑑定者': ,
    # '鑑定者': ,
    # '鑑定日期': ,
    # --- 地點與環境 ---
    '國名': 'locality_1',
    '行政區域': 'locality_1',
    '省/縣名':  'locality_2',
    '採集地':   'locality_3',
    '採集地名': 'locality_3',
    '採集地 Locality':  'locality_3',
    '經緯度 Coordinates': 'Coordinates',
    # '海拔': ,
    # '海拔 Altitude': ,
    # '海拔高度(m)': ,
    # '基質 Substrate': ,
    # --- 標本屬性 ---
    '保存方式': 'preservation',
    '數量': 'organismQuantity',
    '保存液': 'preservation',
    '固定方式': 'preservation',
    # '菌株 Culture':
    # 'DNA': 
}


field_list = list(set(field_map.values()))


now = datetime.now() + timedelta(hours=8) 


for now_category in category_list[category_index:]:
    has_more_data = True
    data = []
    all_count = 0
    while has_more_data:
        # 須在server上執行
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
            # Step A: 先依據學門給予預設的 Kingdom
            if now_category in kingdom_map:
                now_dict['sourceKingdom'] = kingdom_map[now_category]
            # --- 處理 MetaData_NMNS ---
            for rr in r.get('MetaData_NMNS'):
                caption = rr.get('Caption')
                value = rr.get('Value')
                # 1. 一般欄位處理
                if caption in field_map.keys():
                    now_dict[field_map[caption]] = value
                # 2. 特殊欄位：類別 (處理非維管束學門的 Kingdom)
                elif caption == '類別' and now_category == '非維管束學門' and value:
                    if '苔蘚' in value:
                        now_dict['sourceKingdom'] = 'Plantae'
                    elif '地衣' in value:
                        now_dict['sourceKingdom'] = 'Fungi'
                    # 若是藻類，這裡不做動作，保持沒有 sourceKingdom 的狀態
                # 3. 特殊欄位：分類資訊 Taxonomy
                elif caption == '分類資訊 Taxonomy' and value:
                    # 統一處理：全形逗號切割、去除空白、轉首字大寫
                    tax_parts = [x.strip().title() for x in value.split('，')]
                    if now_category == '真菌學門':
                        # 真菌邏輯 (4層)：門, 綱, 目, 科
                        # if len(tax_parts) > 0: now_dict['sourcePhylum'] = tax_parts[0] # 不取門
                        if len(tax_parts) > 1: now_dict['sourceClass'] = tax_parts[1]
                        if len(tax_parts) > 2: now_dict['sourceOrder'] = tax_parts[2]
                        if len(tax_parts) > 3: now_dict['sourceFamily'] = tax_parts[3]
                    elif now_category == '非維管束學門': 
                        # 藻類邏輯 (3層)：門, 目, 科 (跳過綱)
                        # if len(tax_parts) > 0: now_dict['sourcePhylum'] = tax_parts[0] # 不取門
                        if len(tax_parts) > 1: now_dict['sourceOrder'] = tax_parts[1]
                        if len(tax_parts) > 2: now_dict['sourceFamily'] = tax_parts[2]
            data.append(now_dict)
            # for rr in r.get('MetaData_NMNS'):
            #     if rr.get('Caption') in field_map.keys():
            #         now_dict[field_map[rr.get('Caption')]] = rr.get('Value')
            # data.append(now_dict)
        if len(data) > 9999 or not has_more_data:
            print(now_category, len(data), all_count)
            df = pd.DataFrame(data)
            data = [] # 重新下一個loop
            df = df.replace(to_quote_dict)
            if 'license' in df.keys():
                df = df[(df.license!='無法辨識授權')&(df.license!='')&(~df.license.str.contains('ND|nd',regex=True))]
            # 如果有資料沒有這些欄位 要先幫忙補上去
            for sci_key in ['sourceScientificName', 'sourceVernacularName', 'sourceOrder', 'sourceFamily', 'sourceClass', 'sourceKingdom']:
                if sci_key not in df.keys():
                    df[sci_key] = ''
            # if now_category == '真菌學門':
            #     df['sourceVernacularName'] = ''
            if 'genus' in df.keys() and 'specificEpithet' in df.keys():
                df['sourceScientificName'] = df.apply(lambda x: x.genus + ' ' + x.specificEpithet ,axis=1)
            for fl in field_list:
                if fl not in df.keys():
                    df[fl] = ''
            # 如果學名相關的欄位都是空值才排除
            df = df[~((df.sourceScientificName=='')&(df.sourceVernacularName=='')&(df.sourceOrder=='')&(df.sourceFamily==''))]
            media_rule_list = []
            if len(df):
                df = df.reset_index(drop=True)
                df = df.replace(to_quote_dict)
                # 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
                df['id'] = df.apply(lambda x: str(bson.objectid.ObjectId()), axis=1)
                for col in cols_str_ends:
                    if col in df.keys():
                        df[col] = df[col].apply(check_id_str_ends)
                sci_names = df[sci_cols].drop_duplicates().reset_index(drop=True)
                sci_names['sci_index'] = sci_names.index
                df = df.merge(sci_names)
                match_results = matching_flow_new_optimized(sci_names)
                df = df.drop(columns=['taxonID'], errors='ignore')
                if len(match_results):
                    df = df.merge(match_results[match_cols], on='sci_index', how='left')
                df['datasetName'] = rights_holder + '-' + now_category
                df['sourceModified'] = df['sourceModified'].apply(lambda x: convert_date(x))
                df['group'] = group
                df['rightsHolder'] = rights_holder
                df['created'] = now
                df['modified'] = now
                df['recordType'] = 'col'
                # 數量 
                if 'organismQuantity' in df.keys():
                    df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))
                # basisOfRecord 無資料
                # 敏感層級 無資料
                for i in df.index:
                    row = df.loc[i]
                    # 在這邊處理出現地的問題
                    locality_list = []
                    for locality_ in ['locality', 'locality_1', 'locality_2', 'locality_3']:
                        if row.get(locality_):
                            locality_list.append(row.get(locality_).strip())
                    df.loc[i, 'locality'] = ' '.join(locality_list)
                    if 'associatedMedia' in df.keys():
                        if df.loc[i, 'associatedMedia']:
                            df.loc[i,'mediaLicense'] = 'OGDL' # 幫忙補上mediaLicense
                            media_rule = get_media_rule(df.loc[i, 'associatedMedia'])
                            if media_rule and media_rule not in media_rule_list:
                                media_rule_list.append(media_rule)
                # 年月日
                df[date_keys] = df.apply(lambda x: pd.Series(convert_year_month_day_new(x.to_dict())), axis=1)
                for d_col in ['year','month','day']:
                    if d_col in df.keys():
                        df[d_col] = df[d_col].fillna(0).astype(int).replace({0: None})
                df = df.replace(to_quote_dict)
                df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
                # 地理資訊 - 
                # 目前僅有非維管束學門有經緯度 沒有敏感資料
                if 'Coordinates' in df.keys():
                    df[['verbatimLatitude', 'verbatimLongitude']] = df['Coordinates'].apply(lambda x: pd.Series(parse_verbatim_coords(x)))
                    for g in geo_wo_raw_keys:
                        if g not in df.keys():
                            df[g] = ''
                    df[geo_wo_raw_keys] = df.apply(lambda x: pd.Series(create_grid_data_new(x.verbatimLongitude, x.verbatimLatitude)),  axis=1)
                # 資料集
                ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
                # return tbiaDatasetID 並加上去
                return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version, group=group)
                df = df.merge(return_dataset_id)
                # 取得已建立的tbiaID
                df['catalogNumber'] = df['catalogNumber'].astype('str')
                if 'occurrenceID' not in df.keys():
                    df['occurrenceID'] = ''
                else:
                    df['occurrenceID'] = df['occurrenceID'].astype('str')
                existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])
                existed_records = get_existed_records_optimized(occ_ids=df[df.occurrenceID!='']['occurrenceID'].to_list(), rights_holder=rights_holder, cata_ids=df[df.catalogNumber!='']['catalogNumber'].to_list())
                existed_records = existed_records.replace({nan:''})
                if len(existed_records):
                    df = df.merge(existed_records, how='left')
                    df = df.replace(to_none_dict)
                    df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
                    df = df.drop(columns=['tbiaID'])
                df = df.replace(to_none_dict)
                # 更新match_log
                match_log = df[match_log_cols]
                match_log = match_log.reset_index(drop=True)
                match_log = create_match_log_df(match_log,now)
                matchlog_processor.smart_upsert_match_log(match_log, existed_records=existed_records)
                match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}_{now_category}.csv',index=None)
                # 用tbiaID更新records
                df['is_deleted'] = False
                df['update_version'] = int(update_version)
                df = df.rename(columns=({'id': 'tbiaID'}))
                df = df.drop(columns=[ck for ck in df.keys() if ck not in records_cols],errors='ignore')
                records_processor.smart_upsert_records(df, existed_records=existed_records)
                for mm in media_rule_list:
                    update_media_rule(media_rule=mm,rights_holder=rights_holder)
        # 成功之後 更新update_update_version
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=None, note=json.dumps({'category_index': category_index, 'offset': offset}))
    category_index += 1
    offset = 0
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=None, note=json.dumps({'category_index': category_index, 'offset': offset}))

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