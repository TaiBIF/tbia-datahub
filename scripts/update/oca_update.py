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

from scripts.taxon.match_taibif_utils import matching_flow
from scripts.utils import *


# 比對學名時使用的欄位
sci_cols = ['sourceVernacularName', 'sourceScientificName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily']

# 若原資料庫原本就有提供taxonID 在這段要拿掉 避免merge時產生衝突
df_sci_cols = [s for s in sci_cols if s != 'taxonID'] 

# 在postgres要排除掉的taxon欄位
# psql_records_key = [k for k in taxon.keys() if k != 'taxonID']

# 單位資訊
group = 'oca'
rights_holder = '海洋保育資料倉儲系統'

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


now = datetime.now() + timedelta(hours=8)

payload = {'API-KEY': os.getenv('OCA_KEY')}
headers = {'content-type': 'application/json'}

final_df = pd.DataFrame()

# iocean 目擊回報
# 時間格式 YYYY-MM-DD
url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id=efb09ebd-1191-43be-ab52-80285c61d703"
r = requests.post(url, data=json.dumps(payload), headers=headers)
if r.status_code == 200:
    x = r.text
    x = x.split('\r\n')
    # 前兩行是header
    header = [xx.replace('"','') for xx in x[0].split(',')]
    rows = []
    for rr in x[2:-1] :
        rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
    df = pd.DataFrame(rows, columns=header)
    # df = df.map(lambda x: x.replace('"', ''))
    df = df.drop(columns=['OCA_Sightings_Type'])
    df = df.rename(columns={'ID': 'occurrenceID' ,'Species_Name': 'sourceVernacularName', 'Name_Code': 'scientificNameID', 
                            'Sightings_Time': 'eventDate', 'Sightings_Count': 'organismQuantity', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude', 'Update_Time': 'sourceModified'})
    df = df[~(df.sourceVernacularName.isin([nan,'',None]))]
    df['datasetName'] = 'iOcean海洋生物目擊回報'
    final_df = pd.concat([df,final_df])


# iocean 垂釣回報
url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id=b46468f7-eaff-40ac-96ec-a404ad0bea9f"
r = requests.post(url, data=json.dumps(payload), headers=headers)
if r.status_code == 200:
    x = r.text
    x = x.split('\r\n')
    # 前兩行是header
    header = [xx.replace('"','') for xx in x[0].split(',')]
    rows = []
    for rr in x[2:-1] :
        rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
    df = pd.DataFrame(rows, columns=header)
    # df = df.map(lambda x: x.replace('"', ''))
    df = df.drop(columns=['Type_Name', 'People_Count','Fishing_Length'])
    df = df.rename(columns={'ID': 'occurrenceID', 'Fishing_Time': 'eventDate', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude',
                            'Species_Name': 'sourceVernacularName', 'Name_Code': 'scientificNameID', 
                            'Fishing_Count': 'organismQuantity', 'UpdataTime': 'sourceModified'})
    df = df[~(df.sourceVernacularName.isin([nan,'',None]))]
    df['datasetName'] = 'iOcean垂釣回報'
    final_df = pd.concat([df,final_df])


# ['ID', 'Type_Name', 'Fishing_Time', 'People_Count', 'WGS84X', 'WGS84Y',
#        'UpdataTime', 'Species_Name', 'Name_Code', 'Fishing_Count',
#        'Fishing_Length']

# MARN鯨豚擱淺資料
# 時間格式 2020/1/4 下午 04:39:00 
url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id=571f4642-79d5-49f2-87c6-25a00d05c32e"
r = requests.post(url, data=json.dumps(payload), headers=headers)
if r.status_code == 200:
    x = r.text
    x = x.split('\r\n')
    # 前兩行是header
    header = [xx.replace('"','') for xx in x[0].split(',')]
    rows = []
    for rr in x[2:-1] :
        rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
    df = pd.DataFrame(rows, columns=header)
    # df = df.map(lambda x: x.replace('"', ''))
    df = df.drop(columns=['Status', 'isGroup', 'Length', 'Handle'])
    df = df.rename(columns={'Event_Date': 'eventDate', 'County_Co': 'locality', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude',
                            'Species_Name': 'sourceVernacularName', 'appName': 'recordedBy'})
    df = df[~(df.sourceVernacularName.isin([nan,'',None]))]
    df['datasetName'] = 'MARN鯨豚擱淺資料'
    final_df = pd.concat([df,final_df])


# MARN海龜擱淺資料
url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id=7bd7b385-94d6-4b1e-a16d-08f9bcab031d"
r = requests.post(url, data=json.dumps(payload), headers=headers)
if r.status_code == 200:
    x = r.text
    x = x.split('\r\n')
    # 前兩行是header
    header = [xx.replace('"','') for xx in x[0].split(',')]
    rows = []
    for rr in x[2:-1] :
        rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
    df = pd.DataFrame(rows, columns=header)
    # df = df.map(lambda x: x.replace('"', ''))
    df = df.drop(columns=['Cause','Status', 'Length', 'Width', 'Handle',])
    df = df.rename(columns={'Event_Date': 'eventDate', 'County_Co': 'locality', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude', 
                            'Species_Name': 'sourceVernacularName', 'appName': 'recordedBy'})
    df = df[~(df.sourceVernacularName.isin([nan,'',None]))]
    df['datasetName'] = 'MARN海龜擱淺資料'
    final_df = pd.concat([df,final_df])


# 結構化檔案

d_names = ["珊瑚調查相關結構化檔案","硨磲貝及其他重要螺貝類調查相關結構化檔案","三棘鱟調查相關結構化檔案","軟骨魚調查相關結構化檔案","海鳥調查相關結構化檔案","海鳥調查相關結構化檔案",
"海馬調查相關結構化檔案","鯨豚調查相關結構化檔案","白海豚調查相關結構化檔案","白海豚調查相關結構化檔案","白海豚調查相關結構化檔案","白海豚調查相關結構化檔案",
"海域棲地調查相關結構化檔案","海域棲地調查相關結構化檔案","海域棲地調查相關結構化檔案","海域棲地調查相關結構化檔案","海域棲地調查相關結構化檔案","海域棲地調查相關結構化檔案"]

d_ids = ["553522f0-e39c-46a0-bf78-78e12135d647","4e891143-b14f-4702-928d-bfe9ec3d8f70",
"28dd1c19-98c3-4045-918a-1329f1a71c28","8efa70b8-fe17-498e-8fc1-7f3906d7e75c","00f89ccc-1e97-427c-bd58-5d64ceeaaf16","625d6e09-7ee2-4810-b603-449b99874499",
"1eb771e2-8459-4149-8d8f-156044b44896","48ab136d-1734-45f3-856e-99f7ebf8c96f","f7be4196-5435-4e5e-a335-9a51ccec4d65","4884e92b-151e-4bd7-8dac-d3b7fa873e8d",
"5d0c1d2a-23e9-449d-bd4d-9134b0145859","09d227de-f7e4-4ac5-b2df-bfbf8da40ec8","b40ed072-1ed7-4624-9727-7b628e46c40c","0599360b-6ded-4ae9-af52-9b029a45ef04",
"88c1f6d6-acb5-479f-810b-b9f537cbc560","3f51cb2a-2068-4bbb-b002-78c4320b0108","45a4b4a1-2f7d-4902-9e3e-c1fc7876a42f","63af9fe5-6ea7-4692-b1c5-720481c85500"]

ocas = pd.DataFrame({'d_name': d_names, 'd_id': d_ids})

unused_keys = ['界中文名','門中文名','綱中文名','目中文名','科中文名','屬中文名','\ufeff計畫/案件名稱','計畫/案件名稱','調查方法','所處位置', '備註', '界2','物種俗名','界','門','屬','屬 (Genus)']

for i in ocas.index:
    print(i)
    row = ocas.iloc[i]
    url = f"https://iocean.oca.gov.tw/oca_datahub/WebService/GetData.ashx?id={row.d_id}"
    r = requests.post(url, data=json.dumps(payload), headers=headers)
    if r.status_code == 200:
        x = r.text
        x = x.split('\r\n')
        # 前一行是header
        header = [xx.replace('"','') for xx in x[0].split(',')]
        rows = []
        for rr in x[1:-1] :
            rows.append([ '{}'.format(x) for x in list(csv.reader([rr], delimiter=',', quotechar='"'))[0] ])
        df = pd.DataFrame(rows, columns=header)
        # df = df.map(lambda x: x.replace('"', ''))
        df = df.replace({nan: None, '#N/A': None})
        df = df.rename(columns={'經度': 'verbatimLongitude', '緯度': 'verbatimLatitude', 
                                '直轄市或省轄縣市': 'locality', '縣市': 'locality',
                                '鑑定層級': 'sourceTaxonRank', '原始物種名稱': 'sourceVernacularName', '原始物種名稱 (中文)': 'sourceVernacularName',
                                '原始物種學名': 'sourceScientificName',
                                '校定物種學名編碼': 'scientificNameID', '數量': 'organismQuantity', '數量單位': 'organismQuantityType', 
                                # '界': 'kingdom', '門': 'phylum',
                                '綱': 'sourceClass', '綱 (class)': 'sourceClass', 
                                '目': 'sourceOrder', '目 (Order)': 'sourceOrder', 
                                '科': 'sourceFamily', '科 (Family)': 'sourceFamily',
                                # '屬':'genus', '屬 (Genus)': 'genus',
                                '西元年': 'year', '月': 'month', '日': 'day'})
        # print(df.keys(), i)
        # if '計畫/案件名稱' in df.keys():
        #     print(df['計畫/案件名稱'].unique())
        if '物種俗名' in df.keys():
            df['sourceVernacularName'] = df.apply(lambda x: x.sourceVernacularName + ';' + x.物種俗名 if x.物種俗名 else x.sourceVernacularName, axis=1)
        df['eventDate'] = df.apply(lambda x: f"{x.year}-{x.month}-{x.day}", axis=1) 
        df['datasetName'] = row.d_name
        df = df[~(df.sourceVernacularName.isin([nan,'',None])&df.sourceScientificName.isin([nan,'',None]))] 
        drop_keys = [k for k in df.keys() if k in unused_keys]
        df = df.drop(columns=drop_keys)
        df = df.drop(columns=['drop'], errors='ignore')
        final_df = pd.concat([df,final_df])


# final_df = final_df.drop(columns=['Fishing_Length'])
final_df = final_df.replace({nan:None})

df = final_df

df = df[~(df.sourceVernacularName.isin([nan,'',None])&df.sourceScientificName.isin([nan,'',None]))]
df = df.reset_index(drop=True)
df = df.replace({nan: '', 'NA': '', '-99999': ''})


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


df['recordType'] = 'occ'
df['group'] = group
df['rightsHolder'] = rights_holder
df['created'] = now
df['modified'] = now


# 日期
df['standardDate'] = df['eventDate'].apply(lambda x: convert_date(x))
# 數量
df['standardOrganismQuantity'] = df['organismQuantity'].apply(lambda x: standardize_quantity(x))

# basisOfRecord 無資料
# dataGeneralizations 無資料


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
    df.loc[i,'id'] = str(bson.objectid.ObjectId())
    row = df.loc[i]
    if any(ext in str(row.verbatimLongitude) for ext in ['N', 'S', 'W', 'E']) or any(ext in str(row.verbatimLatitude) for ext in ['N', 'S', 'W', 'E']):
        lon, lat = convert_to_decimal(row.verbatimLongitude, row.verbatimLatitude)
    else:
        lon, lat = row.verbatimLongitude, row.verbatimLatitude
    standardLon, standardLat, location_rpt = standardize_coor(lon, lat)
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

ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
update_dataset_key(ds_name=ds_name, rights_holder=rights_holder)

df = df.replace({np.nan: None, '': None})

# 更新match_log
# 更新資料
if 'occurrenceID' in df.keys():
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
else:
    df['occurrenceID'] = ''

# match_log要用更新的
match_log = df[['occurrenceID','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group','rightsHolder','created','modified']]
match_log = match_log.reset_index(drop=True)
match_log = update_match_log(match_log=match_log, now=now)
match_log.to_csv(f'/portal/media/match_log/{group}_{info_id}.csv',index=None)

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