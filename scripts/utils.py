from datetime import datetime, timedelta
import numpy as np
import bisect
import re
from app import db, db_settings
import psycopg2
import sqlalchemy as sa
import requests
from sqlalchemy.dialects.postgresql import insert
import json
import pandas as pd
from dateutil import parser
import twd97
import pymysql
from dotenv import load_dotenv
import os
import subprocess
import math
from shapely.geometry import Point, Polygon
import geopandas as gpd
import bson
from numpy import nan

gdf = gpd.read_file('/bucket/TW_TOWN/TOWN_MOI_1131028.shp')
gdf_ocean = gpd.read_file('/bucket/TW_TOWN_OCEAN/tw_map_o.shp')

load_dotenv(override=True)

date_keys = ['eventDate','standardDate','year','month','day']

geo_keys = ['verbatimRawLongitude', 'verbatimRawLatitude', 'standardRawLongitude','standardRawLatitude','raw_location_rpt','verbatimLongitude', 'verbatimLatitude', 'standardLongitude','standardLatitude','location_rpt',
                'grid_1','grid_1_blurred','grid_5','grid_5_blurred','grid_10','grid_10_blurred','grid_100','grid_100_blurred','rawCounty','rawMunicipality','county','municipality']

geo_wo_raw_keys = ['verbatimLongitude', 'verbatimLatitude', 'standardLongitude','standardLatitude','location_rpt',
                'grid_1','grid_1_blurred','grid_5','grid_5_blurred','grid_10','grid_10_blurred','grid_100','grid_100_blurred','county','municipality']

taicol_db_settings = {
    "host": os.getenv('TaiCOL_DB_HOST'),
    "port": int(os.getenv('TaiCOL_DB_PORT')),
    "user": os.getenv('TaiCOL_DB_USER'),
    "password": os.getenv('TaiCOL_DB_PASSWORD'),
    "database": os.getenv('TaiCOL_DB_DBNAME'),
}


rights_holder_map = {
    'GBIF': 'gbif',
    '中央研究院生物多樣性中心動物標本館': 'asiz',
    '中央研究院生物多樣性中心植物標本資料庫': 'hast',
    '台灣生物多樣性網絡 TBN': 'tbri',
    '國立臺灣博物館典藏': 'ntm',
    '林業試驗所昆蟲標本館': 'fact',
    '林業試驗所植物標本資料庫': 'taif',
    '河川環境資料庫': 'wra',
    '濕地環境資料庫': 'nps',
    '生態調查資料庫系統': 'forest',
    '臺灣國家公園生物多樣性資料庫': 'nps',
    '臺灣生物多樣性資訊機構 TaiBIF': 'brcas',
    '海洋保育資料倉儲系統': 'oca',
    '科博典藏 (NMNS Collection)': 'nmns',
    '臺灣魚類資料庫': 'ascdc',
}



to_none_dict = {nan: None, 
                'NA': None, 
                '-99999': None, 
                '-999999': None, 
                'N/A': None, 
                'nan': None, 
                '': None}

to_quote_dict = {nan: '', 
                 'NA': '', 
                 '-99999': '', 
                 '-999999': '', 
                 'N/A': '', 
                 'nan': '',
                 None: ''}



with db.begin() as conn:
    qry = sa.text("SELECT column_name FROM information_schema.columns WHERE table_name = 'records';")
    resultset = conn.execute(qry)
    records_cols = [r[0] for r in resultset.all()]


# 和資料庫裡有差三個欄位: 'is_matched', 'rights_holder', 'tbiaID'
match_log_cols = ['occurrenceID','catalogNumber','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','group','rightsHolder','created','modified']


# taxon_group_map = {
#     'Insects' : [{'key': 'class', 'value': 'Insecta'}],
#     'Fishes' : [{'key': 'superclass', 'value': 'Actinopterygii'},{'key': 'superclass', 'value': 'Chondrichthyes'},{'key': 'class', 'value': 'Myxini'}],
#     'Reptiles' : [{'key': 'class', 'value': 'Reptilia'}],
#     'Fungi' : [{'key': 'kingdom', 'value': 'Fungi'}],
#     'Plants' : [{'key': 'kingdom', 'value': 'Plantae'}],
#     'Birds' : [{'key': 'class', 'value': 'Aves'}],
#     'Mammals' : [{'key': 'class', 'value': 'Mammalia'}],
#     'Amphibians' : [{'key': 'class', 'value': 'Amphibia'}],
#     'Bacteria' : [{'key': 'kingdom', 'value': 'Bacteria'}],
#     'Others' : [{'key': 'class', 'value': ''}],
# }


issue_map = {
    1: 'higherrank',
    2: 'none',
    3: 'fuzzy',
    4: 'multiple'
}


date_formats = ['%Y/%m/%d','%Y%m%d','%Y-%m-%d','%Y/%m/%d %H:%M:%S','%Y-%m-%d %H:%M',
                '%Y/%m/%d %H:%M','%Y-%m-%d %H:%M:%S','%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d %p %I:%M:%S', '%Y/%m/%d %H', '%Y-%m-%d %H', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ', '%Y0%m0%d']


basis_dict = {
    "人為觀測": "HumanObservation",
    "機器觀測": "MachineObservation",
    "保存標本": "PreservedSpecimen",
    "材料樣本": "MaterialSample",
    "活體標本": "LivingSpecimen",
    "化石標本": "FossilSpecimen",
    "文獻紀錄": "MaterialCitation",
    "材料實體": "MaterialEntity",
    "分類群": "Taxon",
    "出現紀錄": "Occurrence",
    "調查活動": "Event",
    "材料引用": "MaterialCitation", # GBIF資料
    "組織樣本": "MaterialSample", # GBIF資料
    "人類調查": "HumanObservation", # GBIF資料,
    "Camera": "MachineObservation",
    "CameraTrap": "MachineObservation",
    "Event": "Event",
    "FossilSpecimen": "FossilSpecimen",
    "Human observation": "HumanObservation",
    "HumanObeservation": "HumanObservation",
    "HumanObservatio": "HumanObservation",
    "HumanObservation": "HumanObservation",
    "LivingSpecimen": "LivingSpecimen",
    "MachineObservation": "MachineObservation",
    "MachineObservation ": "MachineObservation",
    "MachineObservation (機器觀測)": "MachineObservation",
    "MaterialCitation": "MaterialCitation",
    "MaterialSample": "MaterialSample",
    "Occurrence": "Occurrence",
    "PreservedSpecimen": "PreservedSpecimen",
    "camera record": "MachineObservation",
    "event": "Event",
    "occurrence": "Occurrence",
    "出現記錄": "Occurrence",
    "機械觀測": "MachineObservation"
}


def format_float(num):
    try:
        num = float(num)
        num = np.format_float_positional(num, trim='-')
    except:
        num = None
    return num


def get_namecode(namecode):
    conn = pymysql.connect(**taicol_db_settings)
    with conn.cursor() as cursor:     
        query = """
        WITH cte
            AS
            (
                SELECT distinct anc.namecode, anc.taxon_name_id, atu.taxon_id, atu.status, at.is_deleted
                FROM api_namecode anc
                LEFT JOIN api_taxon_usages atu ON atu.taxon_name_id = anc.taxon_name_id
                LEFT JOIN api_taxon at ON at.taxon_id = atu.taxon_id
                WHERE anc.namecode = %s
            )
        SELECT namecode, taxon_name_id, 
        JSON_ARRAYAGG(JSON_OBJECT('taxon_id', taxon_id, 'status', status, 'is_deleted', is_deleted))
        FROM cte GROUP BY namecode, taxon_name_id;
        """
        cursor.execute(query, (namecode))
        df = pd.DataFrame(cursor.fetchall(), columns=['namecode', 'name_id', 'taxon'])
        for i in df.index:
            row = df.iloc[i]
            taxon_tmp = json.loads(row.taxon)
            taxon_final = []
            for t in taxon_tmp:
                if t.get('is_deleted'):
                    taxon_final.append({'taxon_id': t.get('taxon_id'), 'usage_status': 'deleted'})
                elif t.get('taxon_id'):
                    taxon_final.append({'taxon_id': t.get('taxon_id'), 'usage_status': t.get('status')})
            df.loc[i,'taxon'] = json.dumps(taxon_final)
        if len(df):
            df['taxon'] = df['taxon'].replace({np.nan:'[]'})
            df['taxon'] = df['taxon'].apply(json.loads)
        return df.to_dict('records')


def get_existed_records(occ_ids, rights_holder, get_reference=False, cata_ids=[]):

    if occ_ids:
        occ_ids = [f'"{d}"' for d in occ_ids]

    if cata_ids:
        cata_ids = [f'"{d}"' for d in cata_ids]

    subset_list = []
    get_fields = ['id', 'occurrenceID', 'catalogNumber']

    if get_reference:
        get_fields.append('references')

    for tt in range(0, len(occ_ids), 20):
        query = { "query": "*:*",
                "offset": 0,
                "filter": [f"rightsHolder:{rights_holder}", f"occurrenceID:({' OR '.join(occ_ids[tt:tt+20])})"],
                "limit": 1000000,
                "fields": get_fields
                }
        response = requests.post(f'http://solr:8983/solr/tbia_records/select', data=json.dumps(query), headers={'content-type': "application/json" })
        if response.status_code == 200:
            resp = response.json()
            if data := resp['response']['docs']:
                subset_list += data

    for tt in range(0, len(cata_ids), 20):
        query = { "query": "*:*",
                "offset": 0,
                "filter": [f"rightsHolder:{rights_holder}", f"catalogNumber:({' OR '.join(cata_ids[tt:tt+20])})"],
                "limit": 1000000,
                "fields": get_fields
                }
        response = requests.post(f'http://solr:8983/solr/tbia_records/select', data=json.dumps(query), headers={'content-type': "application/json" })
        if response.status_code == 200:
            resp = response.json()
            if data := resp['response']['docs']:
                subset_list += data

    if len(subset_list):
        existed_records = pd.DataFrame(subset_list)
        existed_records = existed_records.drop_duplicates()
        existed_records = existed_records.reset_index(drop=True)
        existed_records = existed_records.rename(columns={'id': 'tbiaID'})
        # 排除掉一個occurrenceID對到多個tbiaID的情況
        # 這邊要多考慮catalogNumber的情況
        for kk in ['occurrenceID', 'catalogNumber', 'references']:
            if kk not in existed_records.keys():
                existed_records[kk] = ''
        existed_records = existed_records.replace({np.nan: '', None: ''})
        a = existed_records[['occurrenceID','tbiaID','catalogNumber']].groupby(['occurrenceID','catalogNumber'], as_index=False).count()
        a = a[a.tbiaID==1]
        # 只保留一對一的結果 若有一對多 則刪除舊的 給予新的tbiaID
        a = a.drop(columns=['tbiaID'])
        existed_records = existed_records.merge(a)
        existed_records = existed_records.reset_index(drop=True)
        if get_reference: # for GBIF ID 使用
            existed_records = existed_records[['tbiaID', 'occurrenceID', 'catalogNumber', 'references']]
        else:
            existed_records = existed_records[['tbiaID', 'occurrenceID', 'catalogNumber']]
    else:
        # existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID','datasetName'])
        existed_records = pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])

    return existed_records


def get_taxon_df(taxon_ids):

    subset_taxon_list = []
    ids = [f"id:{d}" for d in taxon_ids]

    for tt in range(0, len(taxon_ids), 20):
        taxa_query = {'query': " OR ".join(ids[tt:tt+20]), 'limit': 20}
        response = requests.post(f'http://solr:8983/solr/taxa/select', data=json.dumps(taxa_query), headers={'content-type': "application/json" })
        if response.status_code == 200:
            resp = response.json()
            if data := resp['response']['docs']:
                subset_taxon_list += data

    taxon = pd.DataFrame(subset_taxon_list)
    taxon = taxon.rename(columns={'id': 'taxonID'})
    # 這邊要拿掉 _taxonID的欄位才對
    taxon = taxon[taxon.columns.drop(list(taxon.filter(regex='_taxonID')))]
    taxon = taxon.drop(columns=['taxon_name_id','_version_'],errors='ignore')
    taxon = taxon.replace({np.nan:None})

    return taxon

def convert_date(date):
    formatted_date = None
    if date != '' and date is not None:
        date = str(date)
        date = date.replace('上午','AM').replace('下午','PM')
        for ff in date_formats:
            try:
                formatted_date = datetime.strptime(date, ff)
                break
            except:
                formatted_date = None
        if not formatted_date:
            try:
                formatted_date = parser.parse(date)
            except:
                formatted_date = None
        if not formatted_date:
            try: 
                date = date.split('T')[0]
                formatted_date = datetime.strptime(date, '%Y-%m-%d')
                # return formatted_date
            except:
                formatted_date = None        
        if not formatted_date:
            try:
                formatted_date = datetime.fromtimestamp(int(date))
                # return formatted_date
            except:
                formatted_date = None
        if formatted_date:
            formatted_year = formatted_date.year
            if str(formatted_year) not in date:
                formatted_date = None
            # 如果超過當下時間就拿掉
            if formatted_date:
                if formatted_date > datetime.now():
                    formatted_date = None
    return formatted_date


# def convert_year_month_day(row):
#     eventDate = row.eventDate
#     standardDate, year, month, day = None, None, None, None
#     if standardDate := convert_date(eventDate):
#         year = standardDate.year
#         month = standardDate.month
#         day = standardDate.day
#     elif row.get('year') and row.get('month') and row.get('day'):
#         try:
#             year = int(row.get('year'))
#             month = int(row.get('month'))
#             day = int(row.get('day'))
#             if try_eventDate := convert_date('{}-{}-{}'.format(row.get('year'),row.get('month'),row.get('day'))):
#                 year = try_eventDate.year
#                 month = try_eventDate.month
#                 day = try_eventDate.day
#                 eventDate = '{}-{}-{}'.format(year,month,day)
#                 standardDate = try_eventDate
#         except:
#             pass
#     return eventDate, standardDate, year, month, day



def convert_year_month_day_new(row):
    eventDate = row.get('eventDate')
    standardDate, year, month, day = None, None, None, None
    if standardDate := convert_date(eventDate):
        year = standardDate.year
        month = standardDate.month
        day = standardDate.day
    elif row.get('year') and row.get('month') and row.get('day'):
        try:
            year = int(row.get('year'))
            month = int(row.get('month'))
            day = int(row.get('day'))
            if try_eventDate := convert_date('{}-{}-{}'.format(row.get('year'),row.get('month'),row.get('day'))):
                year = try_eventDate.year
                month = try_eventDate.month
                day = try_eventDate.day
                eventDate = '{}-{}-{}'.format(year,month,day)
                standardDate = try_eventDate
        except:
            pass
    return [eventDate, standardDate, year, month, day]



def convert_coor_to_grid(x, y, grid):
    list_x = np.arange(-180, 180+grid, grid)
    list_y = np.arange(-90, 90+grid, grid)
    grid_x = bisect.bisect(list_x, x)-1
    grid_y = bisect.bisect(list_y, y)-1
    return grid_x, grid_y



# N, S, W, E
# E 121° 35.405 - 
# 119°36'62.8\"E
def convert_to_decimal(lon, lat):
    try:
        deg, minutes, seconds, direction =  re.split('[°\'\"]', lat)
        # seconds = seconds[:-1]
        # direction = seconds[-1]
        lat = (float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in ['W', 'S'] else 1)
    except:
        lat = None
    try:
        deg, minutes, seconds, direction =  re.split('[°\'\"]', lon)
        # seconds = seconds[:-1]
        # direction = seconds[-1]
        lon = (float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in ['W', 'S'] else 1)
    except:
        lon = None
    return lon, lat


def standardize_coor(lon,lat):
    try:
        standardLon = float(lon) if lon not in ['', None, '0', 'WGS84'] else None
    except:
        standardLon = None
    if standardLon:
        if not (-180 <= standardLon  and standardLon <= 180):
            standardLon = None
    try:
        standardLat = float(lat) if lat not in ['', None] else None
    except:
        standardLat = None
    if standardLat:
        if not (-90 <= standardLat and standardLat <= 90):
            standardLat = None
    # TWD97的情況
    if not standardLat or not standardLon:
        try:
            standardLat, standardLon = twd97.towgs84(float(lon), float(lat))
            if not (-180 <= standardLon  and standardLon <= 180):
                standardLon = None
            if not (-90 <= standardLat and standardLat <= 90):
                standardLat = None
        except:
            pass
    if standardLon and standardLat:
        # if -180 <= standardLon  and standardLon <= 180 and -90 <= standardLat and standardLat <= 90:
        location_rpt = f'POINT({standardLon} {standardLat})' 
    else:
        location_rpt = None
    return standardLon, standardLat, location_rpt


def standardize_quantity(organismQuantity, individualCount=None):
    quantity = None

    try:
        if individualCount:
            quantity = int(individualCount)
        elif organismQuantity:
            quantity = float(organismQuantity)
    except:
        pass

    return quantity


def control_basis_of_record(basisOfRecord):
    if basisOfRecord in basis_dict.keys():
        basisOfRecord = basis_dict[basisOfRecord]
    else:
        basisOfRecord = ''
    return basisOfRecord

def update_dataset_key(ds_name, rights_holder, update_version):
    # 202404 這邊不需要考慮record_type了
    # TODO 不考慮record_type的話 可能會把原本deprecated的資料集打開 -> 應該改成直接刪掉deprecated的資料集 如果是之前重複的話
    # 先確定之前的search_query再次查詢會不會有問題  or 只考慮把有兩個重複的資料集拿掉
    now = datetime.now() + timedelta(hours=8)
    conn = psycopg2.connect(**db_settings)
    for r in ds_name:
        tbiaDatasetID = 'd' + str(bson.objectid.ObjectId()) # 如果沒有的話新增
        sourceDatasetID = r.get('sourceDatasetID', '')
        datasetURL = r.get('datasetURL')
        gbifDatasetID = r.get('gbifDatasetID')
        datasetLicense = r.get('datasetLicense')
        datasetPublisher = r.get('datasetPublisher')
        # 如果有sourceDatasetID 優先以 sourceDatasetID 更新
        # 但也有可能現在有sourceDatasetID 之前沒有
        if sourceDatasetID:
            query ="""
                DO $$
                BEGIN
                IF EXISTS(SELECT * FROM dataset WHERE "sourceDatasetID" = %s AND rights_holder = %s )
                    THEN
                        UPDATE dataset SET "name" = %s, "gbifDatasetID" = %s, "datasetURL" = %s, deprecated = %s, modified = %s, update_version = %s,
                                           "datasetLicense" = %s, "datasetPublisher" = %s
                                        WHERE "sourceDatasetID" = %s AND rights_holder = %s;
                ELSIF EXISTS(SELECT * FROM dataset WHERE "name" = %s AND rights_holder = %s )
                    THEN
                        UPDATE dataset SET "sourceDatasetID" = %s, "gbifDatasetID" = %s, "datasetURL" = %s, deprecated = %s, modified = %s, update_version = %s,
                                           "datasetLicense" = %s, "datasetPublisher" = %s
                                       WHERE "name" = %s AND rights_holder = %s;
                ELSE 
                    INSERT INTO dataset ("rights_holder", "name", "sourceDatasetID", 
                    "datasetURL","gbifDatasetID", "update_version", "deprecated", created, modified, "datasetLicense", "datasetPublisher", "tbiaDatasetID")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                END IF;
                END $$;
            """
            cur = conn.cursor()
            cur.execute(query, (sourceDatasetID, rights_holder, # condition
                                r.get('datasetName'), gbifDatasetID, r.get('datasetURL'), False, now, update_version, datasetLicense, datasetPublisher, # update
                                sourceDatasetID, rights_holder, # condition
                                r.get('datasetName'), rights_holder, # condition
                                sourceDatasetID, gbifDatasetID, r.get('datasetURL'), False, now, update_version, datasetLicense, datasetPublisher,  # update
                                r.get('datasetName'), rights_holder, # condition
                                rights_holder, r.get('datasetName'),  sourceDatasetID, datasetURL, # insert
                                gbifDatasetID, update_version, False, now, now, datasetLicense, datasetPublisher, tbiaDatasetID))
            conn.commit()
        else:
            # 如果沒有sourceDatasetID 以 datasetName 更新
            query ="""
                DO $$
                BEGIN
                IF EXISTS(SELECT * FROM dataset WHERE "name" = %s AND rights_holder = %s ) 
                THEN
                    UPDATE dataset SET "sourceDatasetID" = %s, "gbifDatasetID" = %s, "datasetURL" = %s, deprecated = %s, modified = %s, update_version = %s,
                        "datasetLicense" = %s, "datasetPublisher" = %s
                        WHERE "name" = %s AND rights_holder = %s ;
                ELSE 
                    INSERT INTO dataset ("rights_holder", "name", "sourceDatasetID", 
                    "datasetURL","gbifDatasetID", "update_version", "deprecated", created, modified, "datasetLicense", "datasetPublisher", "tbiaDatasetID") 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                END IF;
                END $$;
            """
            cur = conn.cursor()
            cur.execute(query, (r.get('datasetName'), rights_holder,  # condition
                                r.get('sourceDatasetID'), gbifDatasetID, r.get('datasetURL'), False, now, update_version, datasetLicense, datasetPublisher,  # update
                                r.get('datasetName'), rights_holder,   # condition
                                rights_holder, r.get('datasetName'),  sourceDatasetID, datasetURL,  # insert
                                gbifDatasetID, update_version, False, now, now, datasetLicense, datasetPublisher, tbiaDatasetID))
            conn.commit()
    dataset_ids = []
    query = '''SELECT "tbiaDatasetID", "name", "sourceDatasetID" FROM dataset WHERE rights_holder = %s AND deprecated = 'f' ''';
    with conn.cursor() as cursor:     
        cursor.execute(query, (rights_holder, ))
        dataset_ids = pd.DataFrame(cursor.fetchall(), columns=['tbiaDatasetID', 'datasetName', 'sourceDatasetID'])
        dataset_ids = dataset_ids.replace({None: '', np.nan: ''})
        dataset_ids = dataset_ids.merge(pd.DataFrame(ds_name))
    conn.close()
    if len(dataset_ids):
        dataset_ids = dataset_ids[['tbiaDatasetID', 'datasetName', 'sourceDatasetID']].drop_duplicates()
    return dataset_ids



# deprecated
# def update_dataset_info(rights_holder):
#     # 改成直接用records裡面的groupby

#     query = '''SELECT count(*), string_agg(distinct "resourceContacts", ','),
#                 string_agg(distinct "recordType", ','), "tbiaDatasetID" FROM records WHERE  "rightsHolder" = %s
#                 GROUP BY "tbiaDatasetID";
#             '''
    
#     conn = psycopg2.connect(**db_settings)

#     with conn.cursor() as cursor:     
#         cursor.execute(query, (rights_holder,))
#         df = pd.DataFrame(cursor.fetchall(), columns=['occurrenceCount','resourceContacts', 'record_type', 'tbiaDatasetID'])
#         df = df.replace({np.nan: '', None: ''})

#     if len(df):
#         df['occurrenceCount'] = df['occurrenceCount'].replace({'': 0})
#         # 更新
#         for i in df.index:
#             row = df.iloc[i]
#             with conn.cursor() as cursor:     
#                 update_query = ''' UPDATE dataset SET "resourceContacts"='{}', "occurrenceCount"={}, "record_type"='{}' WHERE id = {} '''.format(row.resourceContacts, row.occurrenceCount, row.record_type, row.tbiaDatasetID)
#                 execute_line = cursor.execute(update_query)
#                 conn.commit()


def matchlog_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    set_list = ['sourceScientificName','is_matched','taxonID','match_higher_taxon','match_stage',
                'stage_1','stage_2','stage_3','stage_4','stage_5','modified']
    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"matchlog_unique",
        # 如果重複的時候，需要update的欄位
        set_={c.key: c for c in insert_statement.excluded if c.key in set_list},
    )
    conn.execute(upsert_statement)


def update_match_log(match_log, now):
    match_log['is_matched'] = False
    match_log.loc[match_log.taxonID.notnull(),'is_matched'] = True
    match_log = match_log.replace({np.nan: None})
    match_log['match_higher_taxon'] = match_log['match_higher_taxon'].replace({None: False, np.nan: False, '': False})
    match_log['match_stage'] = match_log['match_stage'].apply(lambda x: int(x) if x or x == 0 else None)
    match_log['stage_1'] = match_log['stage_1'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_2'] = match_log['stage_2'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_3'] = match_log['stage_3'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_4'] = match_log['stage_4'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_5'] = match_log['stage_5'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_6'] = match_log['stage_6'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_7'] = match_log['stage_7'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_8'] = match_log['stage_8'].apply(lambda x: issue_map[x] if x else x)
    match_log['created'] = now
    match_log['modified'] = now
    match_log = match_log.rename(columns={'id': 'tbiaID','rightsHolder':'rights_holder'})
    for l in range(0, len(match_log), 1000):
        match_log[l:l+1000].to_sql('match_log', db,
                if_exists='append',
                index=False,
                method=matchlog_upsert)
    return match_log


# def get_records(rights_holder, min_id, limit=10000):
#     with db.begin() as conn:
#         qry = sa.text("""select * from records  
#                         where "rightsHolder" = '{}' AND id > {} order by id limit {}  """.format(rights_holder, min_id, limit)) 
#         resultset = conn.execute(qry)
#         results = resultset.mappings().all()
#         return results
    

def get_gbif_id(gbifDatasetID, occurrenceID):
    gbif_url = f"https://api.gbif.org/v1/occurrence/{gbifDatasetID}/{occurrenceID}"
    gbif_resp = requests.get(gbif_url)
    gbifID = None
    if gbif_resp.status_code == 200:
        gbif_res = gbif_resp.json()
        gbifID = gbif_res.get('gbifID')
    return gbifID



# 更新資料庫內的records

def records_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    # 如果重複的時候，不要update的欄位
    not_set_list = ['created', 'tbiaID']
    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"records_unique",
        set_={c.key: c for c in insert_statement.excluded if c.key not in not_set_list},
    )
    conn.execute(upsert_statement)


def zip_match_log(group, info_id):
    zip_file_path = f'/portal/media/match_log/{group}_{info_id}_match_log.zip'
    csv_file_path = f'{group}_{info_id}*.csv'
    commands = f"cd /portal/media/match_log/; zip -j {zip_file_path} {csv_file_path}; rm {csv_file_path}"
    process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 等待檔案完成
    a = process.communicate()
    return a


def delete_records(rights_holder,group,update_version):
    # 刪除is_deleted的records & match_log
    query = """
                WITH moved_rows AS (
                    DELETE FROM records a
                    WHERE a.update_version != {} and a."rightsHolder" = '{}' and a."group" = '{}'
                    RETURNING a."tbiaID", a."occurrenceID", a."rightsHolder", a."group", a."catalogNumber"
                ), delete_match_log AS (
                    DELETE FROM match_log 
                    WHERE "tbiaID" IN (select "tbiaID" from moved_rows)
                )
                INSERT INTO deleted_records ("tbiaID", "occurrenceID","rights_holder", "group", "catalogNumber", "deleted")
                SELECT *, NOW() as deleted FROM moved_rows;
                """.format(update_version, rights_holder, group)
    conn = psycopg2.connect(**db_settings)
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        conn.commit()
    return execute_line


def update_dataset_deprecated(rights_holder, update_version):
    # update_version不等於這次的 改成 deprecated = 't' 
    conn = psycopg2.connect(**db_settings)
    with conn.cursor() as cursor:     
        update_query = "UPDATE dataset SET deprecated = 't' WHERE rights_holder = '{}' AND update_version != {}".format(rights_holder, update_version)
        execute_line = cursor.execute(update_query)
        conn.commit()



def update_update_version(update_version, rights_holder, current_page=0, note=None, is_finished=False):
    now = datetime.now() + timedelta(hours=8)
    conn = psycopg2.connect(**db_settings)
    if is_finished:
        query = """
                UPDATE update_version SET is_finished = 't', modified = %s WHERE "update_version" = %s and rights_holder = %s;
                """
        cur = conn.cursor()
        cur.execute(query, (now, update_version, rights_holder))
    else:
        query = """
                UPDATE update_version SET current_page = %s, note = %s, modified = %s 
                WHERE "update_version" = %s and rights_holder = %s;
                """
        cur = conn.cursor()
        cur.execute(query, (current_page, note, now, update_version, rights_holder))
    conn.commit()
    conn.close()


def insert_new_update_version(update_version, rights_holder):
    now = datetime.now() + timedelta(hours=8)
    res = None
    conn = psycopg2.connect(**db_settings)
    query = """select * FROM update_version WHERE "update_version" = %s and rights_holder = %s;"""
    with conn.cursor() as cursor:
        cursor.execute(query, (update_version, rights_holder))
        res = cursor.fetchone()
    if res: # 確認是不是有id 有的話代表這個已經存在
        return res[2], res[4] # 回傳page & note
    else:        
        query = """
                    INSERT INTO update_version ("current_page","update_version", rights_holder, created, modified) VALUES (0, %s, %s, %s, %s)
                    """
        cur = conn.cursor()
        cur.execute(query, (update_version, rights_holder, now, now))
        conn.commit()
        conn.close()
        return 0, None



# # 如果是需要幫忙做模糊化的 進來的 orignal_lon & orignal_lat 一定是未模糊化資料
# def create_blurred_grid_data(verbatimLongitude, verbatimLatitude, coordinatePrecision, is_full_hidden=False):
#     # TODO 先暫時不處理科學記號的問題
#     # 判斷coordinatePrecision 是否為合理數值 小於0 or =1 or 完全屏蔽
#     # from DwC quick guide
#     # 0.00001 (normal GPS limit for decimal degrees)
#     # 0.000278 (nearest second) # TODO 尚未處理 還沒有需要處理的資料
#     # 0.01667 (nearest minute) # TODO 尚未處理 還沒有需要處理的資料
#     # 1.0 (nearest degree)
#     standardRawLon, standardRawLat, raw_location_rpt = standardize_coor(verbatimLongitude, verbatimLatitude)
#     grid_data = {}
#     grid_data['grid_1'] = '-1_-1'
#     grid_data['grid_5'] = '-1_-1'
#     grid_data['grid_10'] = '-1_-1'
#     grid_data['grid_100'] = '-1_-1'
#     grid_data['grid_1_blurred'] = '-1_-1'
#     grid_data['grid_5_blurred'] = '-1_-1'
#     grid_data['grid_10_blurred'] = '-1_-1'
#     grid_data['grid_100_blurred'] = '-1_-1'
#     grid_data['standardRawLon'] = standardRawLon
#     grid_data['standardRawLat'] = standardRawLat
#     grid_data['raw_location_rpt'] = raw_location_rpt
#     grid_data['standardLon'] = None
#     grid_data['standardLat'] = None
#     grid_data['location_rpt'] = None
#     if standardRawLon and standardRawLat:
#         if is_full_hidden:
#             fuzzy_lon = None
#             fuzzy_lat = None
#         else:
#             if not coordinatePrecision:
#                 fuzzy_lon = standardRawLon
#                 fuzzy_lat = standardRawLat
#             elif float(coordinatePrecision) < 1 and float(coordinatePrecision) > 0:
#                 ten_times = math.pow(10, len(str(coordinatePrecision).split('.')[-1]))
#                 fuzzy_lon = math.floor(float(standardRawLon)*ten_times)/ten_times
#                 fuzzy_lat = math.floor(float(standardRawLat)*ten_times)/ten_times
#             elif float(coordinatePrecision) == 1:
#                 # 直接去除掉小數點以後的數字
#                 fuzzy_lon = str(standardRawLon).split('.')[0]
#                 fuzzy_lat = str(standardRawLat).split('.')[0]
#             # elif is_full_hidden: # 完全屏蔽 
#             #     fuzzy_lon = None
#             #     fuzzy_lat = None
#             else: # 空值 / 不合理 / 無法判斷
#                 # 直接把 grid_* 跟 grid_*_blurred填入一樣的值
#                 fuzzy_lon = standardRawLon
#                 fuzzy_lat = standardRawLat
#         # 就算沒有給到那麼細的點位 還是一樣畫上去 例如 原始座標只給到121, 21 一樣給一公里網格的資料
#         grid_x, grid_y = convert_coor_to_grid(standardRawLon, standardRawLat, 0.01)
#         grid_data['grid_1'] = str(int(grid_x)) + '_' + str(int(grid_y))
#         grid_x, grid_y = convert_coor_to_grid(standardRawLon, standardRawLat, 0.05)
#         grid_data['grid_5'] = str(int(grid_x)) + '_' + str(int(grid_y))
#         grid_x, grid_y = convert_coor_to_grid(standardRawLon, standardRawLat, 0.1)
#         grid_data['grid_10'] = str(int(grid_x)) + '_' + str(int(grid_y))
#         grid_x, grid_y = convert_coor_to_grid(standardRawLon, standardRawLat, 1)
#         grid_data['grid_100'] = str(int(grid_x)) + '_' + str(int(grid_y))
#         # if fuzzy_lon and fuzzy_lat:
#         standardLon, standardLat, location_rpt = standardize_coor(fuzzy_lon, fuzzy_lat)
#         grid_data['standardLon'] = standardLon
#         grid_data['standardLat'] = standardLat
#         grid_data['location_rpt'] = location_rpt
#         if standardLon and standardLat:
#             grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.01)
#             grid_data['grid_1_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
#             grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.05)
#             grid_data['grid_5_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
#             grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.1)
#             grid_data['grid_10_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
#             grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 1)
#             grid_data['grid_100_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
#     return grid_data


# 如果是需要幫忙做模糊化的 進來的 orignal_lon & orignal_lat 一定是未模糊化資料
def create_blurred_grid_data_new(verbatimLongitude, verbatimLatitude, coordinatePrecision, dataGeneralizations, is_full_hidden=False):
    # 先暫時不處理科學記號的問題
    # 判斷coordinatePrecision 是否為合理數值 小於0 or =1 or 完全屏蔽
    # from DwC quick guide
    # 0.00001 (normal GPS limit for decimal degrees)
    # 0.000278 (nearest second) # 尚未處理 還沒有需要處理的資料
    # 0.01667 (nearest minute) # 尚未處理 還沒有需要處理的資料
    # 1.0 (nearest degree)
    grid_data = {}
    for field in ['grid_1','grid_5','grid_10','grid_100','grid_1_blurred','grid_5_blurred','grid_10_blurred','grid_100_blurred']:
        grid_data[field] = '-1_-1'
    # 先取得未模糊化座標
    # 海保署座標轉換 -> 但現在好像沒有了 (?)
    if any(ext in str(verbatimLongitude) for ext in ['N', 'S', 'W', 'E']) or any(ext in str(verbatimLatitude) for ext in ['N', 'S', 'W', 'E']):
        lon, lat = convert_to_decimal(verbatimLongitude, verbatimLatitude)
    else:
        lon, lat = verbatimLongitude, verbatimLatitude
    standardRawLon, standardRawLat, raw_location_rpt = standardize_coor(lon, lat)
    if dataGeneralizations or is_full_hidden: # 補上原始 (有模糊化座標才會有以下欄位)
        grid_data['standardRawLongitude'] = standardRawLon
        grid_data['standardRawLatitude'] = standardRawLat
        grid_data['raw_location_rpt'] = raw_location_rpt
        grid_data['verbatimRawLongitude'] = verbatimLongitude
        grid_data['verbatimRawLatitude'] = verbatimLatitude
    # 處理模糊化
    if standardRawLon and standardRawLat:
        if is_full_hidden:
            fuzzy_lon = None
            fuzzy_lat = None
        else:
            if not coordinatePrecision: # 如果沒有 coordinatePrecision 就不模糊化
                fuzzy_lon = standardRawLon
                fuzzy_lat = standardRawLat
            elif float(coordinatePrecision) < 1 and float(coordinatePrecision) > 0:
                ten_times = math.pow(10, len(str(coordinatePrecision).split('.')[-1]))
                fuzzy_lon = math.floor(float(standardRawLon)*ten_times)/ten_times
                fuzzy_lat = math.floor(float(standardRawLat)*ten_times)/ten_times
            elif float(coordinatePrecision) == 1:
                # 直接去除掉小數點以後的數字
                fuzzy_lon = str(standardRawLon).split('.')[0]
                fuzzy_lat = str(standardRawLat).split('.')[0]
            else: # 空值 / 不合理 / 無法判斷
                # 直接把 grid_* 跟 grid_*_blurred填入一樣的值
                fuzzy_lon = standardRawLon
                fuzzy_lat = standardRawLat
        # 就算沒有給到那麼細的點位 還是一樣畫上去 例如 原始座標只給到121, 21 一樣給一公里網格的資料
        for grid_level in [1,5,10,100]:
            grid_x, grid_y = convert_coor_to_grid(standardRawLon, standardRawLat, grid_level/100)
            grid_data[f'grid_{grid_level}'] = str(int(grid_x)) + '_' + str(int(grid_y))
        # 不需要模糊化的話 也已經將原始座標帶入fuzzy_lon & fuzzy_lat了
        standardLon, standardLat, location_rpt = standardize_coor(fuzzy_lon, fuzzy_lat)
        grid_data['standardLongitude'] = standardLon
        grid_data['standardLatitude'] = standardLat
        grid_data['location_rpt'] = location_rpt
        if standardLon and standardLat:
            for grid_level in [1,5,10,100]:
                grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, grid_level/100)
                grid_data[f'grid_{grid_level}_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
    # 模糊化 or 完全屏蔽的情況下 原本的 verbatimLongitude & verbatimLatitude 也需要做屏蔽處理
    # 但座標無法轉換的 維持原本的寫法 (not standardLon / not standardLat)
    if grid_data.get('standardLongitude') or is_full_hidden:
        grid_data['verbatimLongitude'] = grid_data.get('standardLongitude')
    else:
        grid_data['verbatimLongitude'] = verbatimLongitude
    if grid_data.get('standardLatitude') or is_full_hidden:
        grid_data['verbatimLatitude'] = grid_data.get('standardLatitude')
    else:
        grid_data['verbatimLatitude'] = verbatimLatitude
    now_point, COUNTYNAME, TOWNNAME  = None, None, None
    if standardRawLon and standardRawLat:
        now_point = Point(standardRawLon, standardRawLat)
    elif grid_data.get('standardLongitude') and grid_data.get('standardLatitude'):
        now_point = Point(grid_data.get('standardLongitude'), grid_data.get('standardLatitude'))
    if now_point:
        if len(gdf[gdf.geometry.contains(now_point)]) == 1:
            COUNTYNAME = gdf[gdf.geometry.contains(now_point)].COUNTYNAME.values[0]
            TOWNNAME = gdf[gdf.geometry.contains(now_point)].TOWNNAME.values[0]
        elif len(gdf_ocean[gdf_ocean.geometry.contains(now_point)]) == 1:
            COUNTYNAME = gdf_ocean[gdf_ocean.geometry.contains(now_point)].COUNTYO.values[0]
    # 確認縣市是否需要屏蔽
    if is_full_hidden: # 這邊應該就會包含 sensitiveCategory in ['縣市','座標不開放']
        grid_data['rawCounty'] = COUNTYNAME
        grid_data['rawMunicipality'] = TOWNNAME
        grid_data['county'] = None
        grid_data['municipality'] = None
    else:
        grid_data['county'] = COUNTYNAME
        grid_data['municipality'] = TOWNNAME
    final_list = []
    for k in geo_keys: # 如果沒有就回會傳 None
        final_list.append(grid_data.get(k))
    return final_list




# 沒有模糊化的情況
def create_grid_data_new(verbatimLongitude, verbatimLatitude):
    grid_data = {}
    for field in ['grid_1','grid_5','grid_10','grid_100','grid_1_blurred','grid_5_blurred','grid_10_blurred','grid_100_blurred']:
        grid_data[field] = '-1_-1'
    standardLon, standardLat, location_rpt = standardize_coor(verbatimLongitude, verbatimLatitude)
    grid_data['standardLongitude'] = standardLon
    grid_data['standardLatitude'] = standardLat
    grid_data['location_rpt'] = location_rpt
    grid_data['verbatimLongitude'] = verbatimLongitude
    grid_data['verbatimLatitude'] = verbatimLatitude
    # 因為沒有模糊化座標 所以grid_* & grid_*_blurred 欄位填一樣的
    if standardLon and standardLat:
        for grid_level in [1,5,10,100]:
            grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, grid_level/100)
            grid_data[f'grid_{grid_level}'] = str(int(grid_x)) + '_' + str(int(grid_y))
            grid_data[f'grid_{grid_level}_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
        now_point, COUNTYNAME, TOWNNAME  = None, None, None
        now_point = Point(standardLon, standardLat)
        if now_point:
            if len(gdf[gdf.geometry.contains(now_point)]) == 1:
                COUNTYNAME = gdf[gdf.geometry.contains(now_point)].COUNTYNAME.values[0]
                TOWNNAME = gdf[gdf.geometry.contains(now_point)].TOWNNAME.values[0]
            elif len(gdf_ocean[gdf_ocean.geometry.contains(now_point)]) == 1:
                COUNTYNAME = gdf_ocean[gdf_ocean.geometry.contains(now_point)].COUNTYO.values[0]
            grid_data['county'] = COUNTYNAME
            grid_data['municipality'] = TOWNNAME
    final_list = []
    for k in geo_wo_raw_keys: # 如果沒有就回會傳 None
        final_list.append(grid_data.get(k))
    return final_list


# 沒有模糊化的情況
def create_grid_data(verbatimLongitude, verbatimLatitude):
    grid_data = {}
    grid_data['grid_1'] = '-1_-1'
    grid_data['grid_5'] = '-1_-1'
    grid_data['grid_10'] = '-1_-1'
    grid_data['grid_100'] = '-1_-1'
    grid_data['grid_1_blurred'] = '-1_-1'
    grid_data['grid_5_blurred'] = '-1_-1'
    grid_data['grid_10_blurred'] = '-1_-1'
    grid_data['grid_100_blurred'] = '-1_-1'
    # grid_data['standardRawLon'] = None
    # grid_data['standardRawLat'] = None
    # grid_data['raw_location_rpt'] = None
    grid_data['standardLon'] = None
    grid_data['standardLat'] = None
    grid_data['location_rpt'] = None
    standardLon, standardLat, location_rpt = standardize_coor(verbatimLongitude, verbatimLatitude)
    grid_data['standardLon'] = standardLon
    grid_data['standardLat'] = standardLat
    grid_data['location_rpt'] = location_rpt
    # 因為沒有模糊化座標 所以grid_* & grid_*_blurred 欄位填一樣的
    if standardLon and standardLat:
        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.01)
        grid_data['grid_1'] = str(int(grid_x)) + '_' + str(int(grid_y))
        grid_data['grid_1_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.05)
        grid_data['grid_5'] = str(int(grid_x)) + '_' + str(int(grid_y))
        grid_data['grid_5_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.1)
        grid_data['grid_10'] = str(int(grid_x)) + '_' + str(int(grid_y))
        grid_data['grid_10_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 1)
        grid_data['grid_100'] = str(int(grid_x)) + '_' + str(int(grid_y))
        grid_data['grid_100_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
    return grid_data


# 取得影像網址前綴
def get_media_rule(media_url):
    full_rule = None
    string_list = media_url.split('//')
    if len(string_list) >= 2:
        protocol = string_list[0]
        domain = string_list[1].split('/')[0]
        full_rule = protocol + '//' + domain
    return full_rule

    # 應該會第一個 / 之前 
    # 例如:
    # https://inaturalist-open-data.s3.amazonaws.com/photos/244585305/large.jpg
    # https://brmas-media.s3.ap-northeast-1.amazonaws.com/hast/specimen/S_092293-l.jpg
    # https://fact.tfri.gov.tw/files/muse_fact/muse_styles/w960_m/mcode/ad080b9a9c3f6a5146f91efcf7c24481.jpg?itok=QwFOo8_E


def update_media_rule(media_rule, rights_holder):
    now = datetime.now() + timedelta(hours=8)
    conn = psycopg2.connect(**db_settings)
    query = """
            INSERT INTO media_rule ("rights_holder", "media_rule", "modified")
            VALUES (%s, %s, %s)
            ON CONFLICT ("rights_holder", "media_rule") DO UPDATE SET modified = %s;
            """
    cur = conn.cursor()
    cur.execute(query, (rights_holder, media_rule, now, now))
    conn.commit()
    conn.close()



# 1 銅: 只要沒有 scientificName、latitude、longitude、year （任何一項）
# 2 銀: 有 scientificName、latitude、longitude、year （這四個都有）
# 3 金: 有 scientificName、latitude、longitude、year、month、coordinatesUncertaintyInMeters / coordinatePrecision 其一 and basisOfRecord 


# 這邊sourceScientificName擴大到originalVernacularName
# basisOfRecord要是控制詞彙

# ['sourceScientificName','originalVernacularName','standardDate','year','month','standardLatitude','standardLongitude','standardRawLatitude','standardRawLongitude','coordinatesUncertaintyInMeters','coordinatePrecision','basisOfRecord']

def calculate_data_quality(row):
    # row = row.to_dict()
    if (row.get('sourceScientificName') or row.get('originalVernacularName')) and (row.get('standardDate') or (row.get('year') and row.get('month'))) and ((row.get('standardLatitude') and row.get('standardLongitude')) or (row.get('standardRawLatitude') and row.get('standardRawLongitude'))) and (row.get('coordinatesUncertaintyInMeters') or row.get('coordinatePrecision')) and ((row.get('basisOfRecord') in basis_dict.keys() )or (row.get('basisOfRecord') in basis_dict.values())):
        data_quality = 3
    elif (row.get('sourceScientificName') or row.get('originalVernacularName')) and (row.get('standardDate') or row.get('year'))  and ((row.get('standardLatitude') and row.get('standardLongitude')) or (row.get('standardRawLatitude') and row.get('standardRawLongitude'))):
        data_quality = 2
    else:
        data_quality = 1
    return data_quality


def return_town(grid_data):
    now_point, COUNTYNAME, TOWNNAME  = None, None, None
    if grid_data.get('standardRawLon') and grid_data.get('standardRawLat'):
        now_point = Point(grid_data.get('standardRawLon'), grid_data.get('standardRawLat'))
    elif grid_data.get('standardLon') and grid_data.get('standardLat'):
        now_point = Point(grid_data.get('standardLon'), grid_data.get('standardLat'))
    if now_point:
        if len(gdf[gdf.geometry.contains(now_point)]) == 1:
            COUNTYNAME = gdf[gdf.geometry.contains(now_point)].COUNTYNAME.values[0]
            TOWNNAME = gdf[gdf.geometry.contains(now_point)].TOWNNAME.values[0]
        elif len(gdf_ocean[gdf_ocean.geometry.contains(now_point)]) == 1:
            COUNTYNAME = gdf_ocean[gdf_ocean.geometry.contains(now_point)].COUNTYO.values[0]
    return COUNTYNAME, TOWNNAME




def coor_precision(row):
    try:
        coordinatePrecision = float(row.coordinatePrecision)
    except:
        coordinatePrecision = None
    if not coordinatePrecision:
        if row.sensitiveCategory == '輕度':
            coordinatePrecision = 0.01
        elif row.sensitiveCategory == '重度':
            coordinatePrecision = 0.1
    return coordinatePrecision


cols_str_ends = ['catalogNumber', 'occurrenceID', 'recordNumber', 'scientificNameID', 'sourceTaxonID', 'sourceOccurrenceID']

# 用在pandas的apply
def check_id_str_ends(now_id):
    try:
        now_id = float(now_id)
        now_id = str(now_id)
        if now_id.endswith('.0'):
            now_id = now_id[:-2]
    except:
        now_id = str(now_id)
    return now_id