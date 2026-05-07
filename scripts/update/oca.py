import requests
import pandas as pd
import time
import csv
from app import engine
from scripts.utils.common import *
from scripts.utils.deduplicates import DedupTracker, resolve_existed_records
from scripts.utils.records import OptimizedRecordsProcessor, prepare_df_for_sql, delete_records
from scripts.utils.match import OptimizedMatchLogProcessor, process_match_log, process_taxon_match, zip_match_log
from scripts.utils.geography import process_geo_batch, geo_keys
from scripts.utils.export import export_records_with_taxon
from scripts.utils.update_version import init_update_session, update_update_version
from scripts.utils.dataset import process_dataset, update_dataset_deprecated
from tqdm import tqdm
from scripts.utils.progress import timer
import atexit

records_processor = OptimizedRecordsProcessor(engine, batch_size=200)
matchlog_processor = OptimizedMatchLogProcessor(engine, batch_size=300)

# 比對學名時使用的欄位
sci_cols = ['taxonID', 'sourceVernacularName','sourceScientificName','scientificNameID','sourceClass','sourceOrder', 'sourceFamily']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'oca'
rights_holder = '海洋保育資料倉儲系統'
info_id = 0

# 更新紀錄
session = init_update_session(rights_holder)
update_version = session.update_version
current_page = session.current_page
note = session.note 
now = session.now
records_processor = session.records_processor
matchlog_processor = session.matchlog_processor

# 更新失敗紀錄
atexit.register(records_processor.export_failed_records, 
                f'failed_records_{group}_{info_id}.csv')
atexit.register(matchlog_processor.export_failed_records, 
                f'failed_match_logs_{group}_{info_id}.csv')


dedup_tracker = DedupTracker(rights_holder, update_version)

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
    df = df.drop(columns=['Status', 'isGroup', 'Body_Length', 'Handle'])
    df = df.rename(columns={'Event_Date': 'eventDate', 'County_Co': 'locality', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude',
                            'Name_Code': 'sourceVernacularName', 'appName': 'recordedBy'})
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
    df = df.drop(columns=['catch','Status', 'Length', 'Width', 'Handle'])
    df = df.rename(columns={'Event_Date': 'eventDate', 'County_Co': 'locality', 
                            'WGS84X': 'verbatimLongitude', 'WGS84Y': 'verbatimLatitude', 
                            'Name_Code': 'sourceVernacularName', 'appAgency': 'recordedBy'})
    df['datasetName'] = 'MARN海龜擱淺資料'
    final_df = pd.concat([df,final_df])


# 結構化檔案
ocas = pd.read_csv('海保署資料集清單.csv')

# 校定物種學名編碼 -> 裡面有可能是taxonID也有可能是namecode 如果不是t開頭的七位數

unused_keys = ['界中文名','門中文名','綱中文名','目中文名','科中文名','屬中文名','\ufeff計畫/案件名稱','計畫/案件名稱','調查方法','調查方式','所處位置', '備註', 
               '界2','物種俗名','門','屬','屬 (Genus)', '中文科別', '屬名', '種名', '站點名稱', '時間(hhmm)', '網目尺寸(mm)', '直徑(cm)', '長度(m)',
               '開始時間(hh:mm)', '結束時間(hh:mm)', '期間(hh:mm)', '流量計數開始', '流量計數結束', '流量差異', '海水量(m3)', '仔稚魚編號', '航次', '深度(m)',
               '所屬計畫類別', '採樣所屬季節', '鑒定者', '覆蓋率', '潮帶位置', '重量', '重量單位', '體長', '體長單位', '體寬', '體寬單位', '性別', '生活史階段',
                '行為', '深度(公尺)', '高度(公尺)', '環境溫度(°C)','觀察記錄起始時間', '觀察記錄結束時間','鑑定者']

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
        # print(df.keys())
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
                                '界': 'sourceKingdom',
                                '西元年': 'year', '年': 'year', '月': 'month', '日': 'day',
                                '大地基準': 'verbatimSRS', '記錄者/採集者': 'recordedBy'})
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


# 因為是完全開放 所以直接帶入CC0
df['license'] = 'CC0' 
# 修改民國年錯誤
mask = (df['datasetName'] == '軟骨魚調查相關結構化檔案')
df.loc[mask & (df['year'].astype(str) == '113'), 'year'] = 2024
df.loc[mask, 'eventDate'] = df.loc[mask, 'eventDate'].str.replace(r'^113-', '2024-', regex=True)

df = filter_by_license_and_sensitivity(df)


if len(df):
    df = df.reset_index(drop=True)
    df = df.replace(to_quote_dict)
    df = process_taxon_match(df, sci_cols)
    df = apply_common_fields(df, group, rights_holder, now)
    df = apply_record_type(df, mode='occ')  # basisOfRecord 無資料
    df, media_rule_list = apply_media_rule(df, [])
    # 根據 datasetName 決定敏感層級（屬於 oca 結構化檔案清單的 → 縣市等級敏感）
    sensitive_mask = df['datasetName'].isin(ocas['d_name'].unique())
    df['dataGeneralizations'] = sensitive_mask
    df['sensitiveCategory'] = sensitive_mask.map({True: '縣市', False: None})
    # 地理資訊
    df[geo_keys] = process_geo_batch(df, is_full_hidden='auto')
    df = df.replace(to_quote_dict)
    df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
    df = process_dataset(df, group, rights_holder, update_version, now)
    df, existed_records = resolve_existed_records(df, rights_holder, dedup_tracker)
    df = df.replace(to_none_dict)
    process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=None)
    df = prepare_df_for_sql(df, update_version)
    records_processor.smart_upsert_records(df, existed_records=existed_records)
    export_records_with_taxon(df, f'/solr/csvs/export/{group}_{info_id}.csv')
    update_media_rules(media_rules=media_rule_list,rights_holder=rights_holder, now=now)


delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
zip_match_log(group=group,info_id=info_id)
update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)
update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)


print('done!')