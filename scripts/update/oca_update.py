import numpy as np
from numpy import nan
import requests
import pandas as pd
import bson
import time
import os
from dateutil import parser
from datetime import datetime, timedelta
import glob
import csv
import json

from scripts.taxon.match_utils import matching_flow_new
from scripts.utils import *


# 比對學名時使用的欄位
sci_cols = ['taxonID', 'sourceVernacularName','sourceScientificName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 


# 單位資訊
group = 'oca'
rights_holder = '海洋保育資料倉儲系統'

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


now = datetime.now() + timedelta(hours=8)

headers = {'content-type': 'application/json','API-KEY': os.getenv('OCA_KEY')}

final_df = pd.DataFrame()

# iocean 目擊回報
# 時間格式 YYYY-MM-DD
url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id=efb09ebd-1191-43be-ab52-80285c61d703"
r = requests.post(url, headers=headers)
if r.status_code == 200:
    x = r.text
    x = x.split('\r\n')
    # 前兩行是header
    header = [xx.replace('"','') for xx in x[0].split(',')]
    rows = []
    for rr in x[2:-1] :
        rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
    df = pd.DataFrame(rows, columns=header)
    df = df.drop(columns=['OCA_Sightings_Type'])
    df = df.rename(columns={'ID': 'occurrenceID' ,'Species_Name': 'sourceVernacularName', 'Name_Code': 'scientificNameID', 
                            'Sightings_Time': 'eventDate', 'Sightings_Count': 'organismQuantity', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude', 'Update_Time': 'sourceModified'})
    df['datasetName'] = 'iOcean海洋生物目擊回報'
    final_df = pd.concat([df,final_df])


# iocean 垂釣回報
url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id=b46468f7-eaff-40ac-96ec-a404ad0bea9f"
r = requests.post(url, headers=headers)
if r.status_code == 200:
    x = r.text
    x = x.split('\r\n')
    # 前兩行是header
    header = [xx.replace('"','') for xx in x[0].split(',')]
    rows = []
    for rr in x[2:-1] :
        rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
    df = pd.DataFrame(rows, columns=header)
    df = df.drop(columns=['Type_Name', 'People_Count','Fishing_Length'])
    df = df.rename(columns={'ID': 'occurrenceID', 'Fishing_Time': 'eventDate', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude',
                            'Species_Name': 'sourceVernacularName', 'Name_Code': 'scientificNameID', 
                            'Fishing_Count': 'organismQuantity', 'UpdataTime': 'sourceModified'})
    df['datasetName'] = 'iOcean垂釣回報'
    final_df = pd.concat([df,final_df])

# MARN鯨豚擱淺資料
# 時間格式 2020/1/4 下午 04:39:00 
url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id=571f4642-79d5-49f2-87c6-25a00d05c32e"
r = requests.post(url, headers=headers)
if r.status_code == 200:
    x = r.text
    x = x.split('\r\n')
    # 前兩行是header
    header = [xx.replace('"','') for xx in x[0].split(',')]
    rows = []
    for rr in x[2:-1] :
        rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
    df = pd.DataFrame(rows, columns=header)
    df = df.drop(columns=['Status', 'isGroup', 'Length', 'Handle'])
    df = df.rename(columns={'Event_Date': 'eventDate', 'County_Co': 'locality', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude',
                            'Species_Name': 'sourceVernacularName', 'appName': 'recordedBy'})
    df['datasetName'] = 'MARN鯨豚擱淺資料'
    final_df = pd.concat([df,final_df])


# MARN海龜擱淺資料
url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id=7bd7b385-94d6-4b1e-a16d-08f9bcab031d"
r = requests.post(url, headers=headers)
if r.status_code == 200:
    x = r.text
    x = x.split('\r\n')
    # 前兩行是header
    header = [xx.replace('"','') for xx in x[0].split(',')]
    rows = []
    for rr in x[2:-1] :
        rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
    df = pd.DataFrame(rows, columns=header)
    df = df.drop(columns=['Cause','Status', 'Length', 'Width', 'Handle',])
    df = df.rename(columns={'Event_Date': 'eventDate', 'County_Co': 'locality', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude', 
                            'Species_Name': 'sourceVernacularName', 'appName': 'recordedBy'})
    df['datasetName'] = 'MARN海龜擱淺資料'
    final_df = pd.concat([df,final_df])


# 結構化檔案

ocas = pd.read_csv('海保署資料集清單_202408.csv')

# 校定物種學名編碼 -> 裡面有可能是taxonID也有可能是namecode 如果不是t開頭的七位數

unused_keys = ['界中文名','門中文名','綱中文名','目中文名','科中文名','屬中文名','\ufeff計畫/案件名稱','計畫/案件名稱','調查方法','調查方式','所處位置', '備註', 
               '界2','物種俗名','界','門','屬','屬 (Genus)', '中文科別', '屬名', '種名', '站點名稱', '時間(hhmm)', '網目尺寸(mm)', '直徑(cm)', '長度(m)',
               '開始時間(hh:mm)', '結束時間(hh:mm)', '期間(hh:mm)', '流量計數開始', '流量計數結束', '流量差異', '海水量(m3)', '仔稚魚編號', '航次', '深度(m)',
               '所屬計畫類別', '採樣所屬季節']

for i in ocas.index:
    row = ocas.iloc[i]
    print(row.project_name)
    df = pd.DataFrame()
    if row.data_type == 'api':
        url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id={row.d_id}"
        r = requests.post(url, headers=headers)
        if r.status_code == 200:
            x = r.text
            x = x.split('\r\n')
            # 前一行是header
            header = [xx.replace('"','') for xx in x[0].split(',')]
            rows = []
            for rr in x[1:-1] :
                rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
            df = pd.DataFrame(rows, columns=header)
    else:
        filename = row.project_name.replace("\t","")
        try:
            df = pd.read_csv(f'oca_dataset/{filename}.csv')
        except:
            pass
    if len(df):
        df = df.replace({nan: None, '#N/A': None, 'nan': None})
        df = df.rename(columns={'經度': 'verbatimLongitude', '緯度': 'verbatimLatitude', 
                                '東經（E）': 'verbatimLongitude', '北緯（N）': 'verbatimLatitude', 
                                'GPS位置-東經（E）': 'verbatimLongitude', 'GPS位置-北緯（N）': 'verbatimLatitude', 
                                '緯度(北緯)': 'verbatimLatitude', '經度(東經)': 'verbatimLongitude',
                                '坐標系統': 'verbatimCoordinateSystem', 
                                '調查日期': 'eventDate', '採樣日期': 'eventDate', '採樣時間':  'eventDate',
                                '直轄市或省轄縣市': 'locality', '縣市': 'locality',
                                '鑑定層級': 'sourceTaxonRank', '原始物種名稱': 'sourceVernacularName', '原始物種名稱 (中文)': 'sourceVernacularName',
                                '中文學名': 'sourceVernacularName',
                                '原始物種學名': 'sourceScientificName', '學名': 'sourceScientificName', '學名(Science Name)': 'sourceScientificName',
                                '校定物種學名編碼': 'scientificNameID', '數量': 'organismQuantity', '數量單位': 'organismQuantityType', 
                                '綱名': 'sourceClass', '綱': 'sourceClass', '綱 (class)': 'sourceClass', 
                                '目名': 'sourceOrder', '目': 'sourceOrder', '目 (Order)': 'sourceOrder', '所屬目別': 'sourceOrder',
                                '科名': 'sourceFamily', '科': 'sourceFamily', '科 (Family)': 'sourceFamily',
                                '科別': 'sourceFamily', '所屬科別': 'sourceFamily', '科別(Family)': 'sourceFamily',
                                '西元年': 'year', '年': 'year', '月': 'month', '日': 'day'})
        # 這邊要處理scientificNameID 裡面可能有一些是taxonID
        if 'scientificNameID' in df.keys():
            df['taxonID'] = df['scientificNameID']
            df['taxonID'] = df['taxonID'].apply(lambda x: x if x and len(str(x)) == 8 and str(x).startswith('t0') else None)
            df['scientificNameID'] = df['scientificNameID'].apply(lambda x: None if x and len(str(x)) == 8 and str(x).startswith('t0') else x)
        if '物種俗名' in df.keys():
            df['sourceVernacularName'] = df['sourceVernacularName'].replace({None: ''})
            df['sourceVernacularName'] = df.apply(lambda x: x.sourceVernacularName + ';' + x.物種俗名 if x.物種俗名 else x.sourceVernacularName, axis=1)
            df['sourceVernacularName'] = df['sourceVernacularName'].apply(lambda x: x.lstrip(';'))
        df['datasetName'] = row.d_name
        drop_keys = [k for k in df.keys() if k in unused_keys]
        df = df.drop(columns=drop_keys)
        df = df.drop(columns=['drop'], errors='ignore')
        final_df = pd.concat([df,final_df])


final_df = final_df.drop(columns=[''],errors='ignore')
final_df = final_df.replace({nan:None, 'nan': None})

df = final_df


if 'sensitiveCategory' in df.keys():
    df = df[~df.sensitiveCategory.isin(['分類群不開放','物種不開放'])]

df = df.reset_index(drop=True)
df = df.replace(to_quote_dict)

for col in cols_str_ends:
    if col in df.keys():
        df[col] = df[col].apply(check_id_str_ends)

df = df[~((df.taxonID=='')&(df.sourceScientificName=='')&(df.sourceVernacularName=='')&(df.scientificNameID=='')&(df.sourceClass=='')&(df.sourceOrder=='')&(df.sourceFamily==''))]

df = df.reset_index(drop=True)
df = df.replace(to_quote_dict)

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


df['recordType'] = 'occ'
df['group'] = group
df['rightsHolder'] = rights_holder
df['created'] = now
df['modified'] = now
df['license'] = 'CC0' # 因為是完全開放 所以直接帶入CC0

# 出現地
if 'locality' in df.keys():
    df['locality'] = df['locality'].apply(lambda x: x.strip() if x else x)

# 數量
df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))

# basisOfRecord 無資料
# dataGeneralizations 無資料

# 先給新的tbiaID，但如果原本就有tbiaID則沿用舊的
df['id'] = df.apply(lambda x: str(bson.objectid.ObjectId()), axis=1)

media_rule_list = []
#  如果有mediaLicense才放associatedMedia
if 'mediaLicense' in df.keys() and 'associatedMedia' in df.keys():
    df['associatedMedia'] = df['associatedMedia'].replace({None: '', np.nan: ''})
    df['associatedMedia'] = df.apply(lambda x: x.associatedMedia if x.mediaLicense else '', axis=1)
    df['media_rule_list'] = df[df.associatedMedia.notnull()]['associatedMedia'].apply(lambda x: get_media_rule(x))
    media_rule_list += list(df[df.media_rule_list.notnull()].media_rule_list.unique())


for i in df.index:
    row = df.loc[i]
    # 敏感資料才需要屏蔽
    if row.datasetName in ocas.d_name.unique():
        # 全部都補上敏感層級
        df.loc[i, 'dataGeneralizations'] = True
        df.loc[i, 'sensitiveCategory'] = '縣市'
    else:
        df.loc[i, 'dataGeneralizations'] = False
        df.loc[i, 'sensitiveCategory'] = None


# 地理資訊
df['is_hidden'] = df.apply(lambda x: True if x.sensitiveCategory in ['縣市','座標不開放'] else False, axis=1)
for g in geo_keys:
    if g not in df.keys():
        df[g] = ''

df['coordinatePrecision'] = None
df[geo_keys] = df.apply(lambda x: pd.Series(create_blurred_grid_data_new(x.verbatimLongitude, x.verbatimLatitude, x.coordinatePrecision, x.dataGeneralizations, is_full_hidden=x.is_hidden)),  axis=1)

# 年月日
df[date_keys] = df.apply(lambda x: pd.Series(convert_year_month_day_new(x.to_dict())), axis=1)
for d_col in ['year','month','day']:
    if d_col in df.keys():
        df[d_col] = df[d_col].fillna(0).astype(int).replace({0: None})

df = df.replace(to_quote_dict)
df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)


# 資料集
ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
# return tbiaDatasetID 並加上去
return_dataset_id = update_dataset_key(ds_name=ds_name, rights_holder=rights_holder, update_version=update_version, group=group)
df = df.merge(return_dataset_id)

df = df.replace(to_quote_dict)
df['catalogNumber'] = ''

# 取得已建立的tbiaID
if 'occurrenceID' in df.keys():
    df['occurrenceID'] = df['occurrenceID'].replace({ None: ''})
    df['occurrenceID'] = df['occurrenceID'].astype('str')
    existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])
    existed_records = get_existed_records(occ_ids=df[df.occurrenceID!='']['occurrenceID'].to_list(), rights_holder=rights_holder, cata_ids=df[df.catalogNumber!='']['catalogNumber'].to_list())
    existed_records = existed_records.replace({nan:''})
    if len(existed_records):
        df = df.merge(existed_records, how='left')
        df = df.replace(to_none_dict)
        df['id'] = df.apply(lambda x: x.tbiaID if x.tbiaID else x.id, axis=1)
        df = df.drop(columns=['tbiaID'])
else:
    df['occurrenceID'] = ''


df = df.replace({to_none_dict})


# 更新match_log
match_log = df[match_log_cols]
match_log = match_log.reset_index(drop=True)
match_log = update_match_log(match_log=match_log, now=now)
match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}.csv',index=None)

# 用tbiaID更新records
df['is_deleted'] = False
df['update_version'] = int(update_version)
df = df.rename(columns=({'id': 'tbiaID'}))
df = df.drop(columns=[ck for ck in df.keys() if ck not in records_cols],errors='ignore')
for l in range(0, len(df), 1000):
    print(l)
    df[l:l+1000].to_sql('records', db,
            if_exists='append',
            index=False,
            method=records_upsert)

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


print('done!')