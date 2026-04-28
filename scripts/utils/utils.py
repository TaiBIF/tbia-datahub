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
import twd97
from dotenv import load_dotenv
import os
import subprocess
import math
from shapely.geometry import Point
import geopandas as gpd
from numpy import nan
import concurrent.futures

gdf = gpd.read_file('/bucket/TW_TOWN/TOWN_MOI_1131028.shp')
gdf_ocean = gpd.read_file('/bucket/TW_TOWN_OCEAN/tw_map_o.shp')

load_dotenv(override=True)


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
    '國家海洋資料庫及共享平台': 'namr',
    '集水區友善環境生態資料庫': 'ardswc',
    '中油生態地圖': 'cpc',
    '作物種原資訊系統': 'npgrc',
    '國立海洋生物博物館生物典藏管理系統': 'nmmba',
}



to_none_dict = {nan: None, 
                'NA': None, 
                '-99999': None, 
                '-999999': None, 
                -99999: None, 
                -999999: None, 
                'N/A': None, 
                'nan': None, 
                '': None}

to_quote_dict = {nan: '', 
                 'NA': '', 
                 '-99999': '', 
                 '-999999': '', 
                 -99999: '', 
                 -999999: '', 
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



def format_float(num):
    try:
        num = float(num)
        num = np.format_float_positional(num, trim='-')
    except:
        num = None
    return num




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


def get_existed_records_optimized(occ_ids, rights_holder, get_reference=False, cata_ids=[], 
                                 batch_size=200, max_workers=4):
    """
    優化版本的 get_existed_records
    主要改進：
    1. 增加批次大小從20到200
    2. 使用並行處理
    3. 減少重複的字符串處理
    
    回傳格式與原版完全一致：包含 tbiaID, occurrenceID, catalogNumber 三個欄位的 DataFrame
    """
    def format_ids(ids):
        return [f'"{d}"' for d in ids] if ids else []
    def query_batch(ids, field_name, rights_holder, get_fields):
        """單次查詢批次"""
        if not ids:
            return []
        query = {
            "query": "*:*",
            "offset": 0,
            "filter": [f"rightsHolder:{rights_holder}", f"{field_name}:({' OR '.join(ids)})"],
            "limit": 1000000,
            "fields": get_fields
        }
        try:
            response = requests.post(
                'http://solr:8983/solr/tbia_records/select', 
                data=json.dumps(query), 
                headers={'content-type': "application/json"},
                timeout=30  # 添加超時
            )
            if response.status_code == 200:
                resp = response.json()
                return resp['response']['docs'] if 'response' in resp and 'docs' in resp['response'] else []
        except Exception as e:
            print(f"Query error: {e}")
        return []
    # 預處理ID列表
    formatted_occ_ids = format_ids(occ_ids)
    formatted_cata_ids = format_ids(cata_ids)
    get_fields = ['id', 'occurrenceID', 'catalogNumber']
    if get_reference:
        get_fields.append('references')
    subset_list = []
    # 使用線程池並行處理
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        # 處理 occurrenceID 批次
        for i in range(0, len(formatted_occ_ids), batch_size):
            batch = formatted_occ_ids[i:i+batch_size]
            future = executor.submit(query_batch, batch, 'occurrenceID', rights_holder, get_fields)
            futures.append(future)
        # 處理 catalogNumber 批次
        for i in range(0, len(formatted_cata_ids), batch_size):
            batch = formatted_cata_ids[i:i+batch_size]
            future = executor.submit(query_batch, batch, 'catalogNumber', rights_holder, get_fields)
            futures.append(future)
        # 收集結果
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                subset_list.extend(result)
            except Exception as e:
                print(f"Future error: {e}")
    # 確保回傳的 DataFrame 有正確的欄位名稱和格式
    if subset_list:
        result_df = pd.DataFrame(subset_list)
        # 將 id 欄位重新命名為 tbiaID 以符合原版格式
        if 'id' in result_df.columns:
            result_df = result_df.rename(columns={'id': 'tbiaID'})
        # 確保包含必要的三個欄位
        required_columns = ['tbiaID', 'occurrenceID', 'catalogNumber']
        for col in required_columns:
            if col not in result_df.columns:
                result_df[col] = ''
        # 如果有 references 欄位且用戶要求，保留它
        if get_reference and 'references' in result_df.columns:
            required_columns.append('references')
        # 只回傳需要的欄位，保持與原版一致的順序
        result_df = result_df[required_columns]
        # 去除重複記錄（可能從不同查詢中獲得相同記錄）
        result_df = result_df.drop_duplicates()
        return result_df
    else:
        # 回傳空的 DataFrame，但包含正確的欄位
        empty_columns = ['tbiaID', 'occurrenceID', 'catalogNumber']
        if get_reference:
            empty_columns.append('references')
        return pd.DataFrame(columns=empty_columns)



def get_taxon_df(taxon_ids):

    subset_taxon_list = []
    ids = [f"id:{d}" for d in taxon_ids]

    for tt in range(0, len(taxon_ids), 500):
        taxa_query = {'query': " OR ".join(ids[tt:tt+500]), 'limit': 500}
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
def convert_coor_to_grid(x, y, grid):
    list_x = np.arange(-180, 180+grid, grid)
    list_y = np.arange(-90, 90+grid, grid)
    grid_x = bisect.bisect(list_x, x)-1
    grid_y = bisect.bisect(list_y, y)-1
    return grid_x, grid_y

def parse_verbatim_coords(coord_str):
    # 如果是空值或非字串，回傳 None
    if pd.isna(coord_str) or str(coord_str).strip() == '':
        return None, None
    text = str(coord_str).strip()
    # 使用正規表示式切割，支援「全形逗號」與「半形逗號」
    # 這裡假設分隔符號是逗號
    parts = re.split(r'[，,]', text)
    # 去除每個部分的空白
    parts = [p.strip() for p in parts if p.strip()]
    v_lat = None
    v_lon = None
    for part in parts:
        upper_part = part.upper()
        # 判斷緯度 (N 或 S)
        if 'N' in upper_part or 'S' in upper_part:
            v_lat = part
        # 判斷經度 (E 或 W)
        elif 'E' in upper_part or 'W' in upper_part:
            v_lon = part
    # 特殊補救：如果有兩段，且其中一段沒找到方向，依照常見順序補齊 (通常是 經度, 緯度)
    # 例如資料中的 '0244748， 121431N' (只有後面有 N)
    if len(parts) == 2:
        if v_lat and not v_lon:
            # 已經找到緯度，剩下那個大概是經度
            v_lon = parts[0] if parts[0] != v_lat else parts[1]
        elif v_lon and not v_lat:
            # 已經找到經度，剩下那個大概是緯度
            v_lat = parts[0] if parts[0] != v_lon else parts[1]
    return v_lat, v_lon


# N, S, W, E
# E 121° 35.405 - 
# 119°36'62.8\"E
# def convert_to_decimal(lon, lat):
#     try:
#         deg, minutes, seconds, direction =  re.split('[°\'\"]', lat)
#         # seconds = seconds[:-1]
#         # direction = seconds[-1]
#         lat = (float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in ['W', 'S'] else 1)
#     except:
#         lat = None
#     try:
#         deg, minutes, seconds, direction =  re.split('[°\'\"]', lon)
#         # seconds = seconds[:-1]
#         # direction = seconds[-1]
#         lon = (float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in ['W', 'S'] else 1)
#     except:
#         lon = None
#     return lon, lat

def convert_to_decimal(lon, lat):
    # 定義一個內部增強版解析函式
    def _parse_one(value):
        # 1. 基本檢查
        if pd.isna(value) or str(value).strip() == '':
            return None
        text = str(value).strip().upper()
        text = text.replace('\\', '').replace('_', '')
        
        # 2. 判斷方向 (S/W 為負)
        # 邏輯：只要字串中有 S 或 W，一律視為負
        sign = 1
        if 'S' in text or 'W' in text:
            sign = -1
        # [安全防護] 若沒有方向字母，但字串以負號開頭 (如 "-120.5")，也視為負
        elif text.strip().startswith('-'):
            sign = -1
            
        # 3. 提取數字 (核心改進)
        # 使用正規表示式抓取所有數字，忽略文字與符號 (N, E, °, ', ", -)
        numbers = re.findall(r"(\d+(?:\.\d*)?|\.\d+)", text)
        
        try:
            nums = [float(n) for n in numbers if n != '.']
        except:
            return None

        if not nums:
            return None
        
        # 4. 自動判斷格式並計算
        val = 0
        if len(nums) == 1:
            # 只有一個數字 -> Decimal Degrees (DD)
            val = nums[0]
        elif len(nums) == 2:
            # 兩個數字 -> 度 + 分 (DM)
            val = nums[0] + nums[1]/60
        elif len(nums) >= 3:
            # 三個以上數字 -> 度 + 分 + 秒 (DMS)
            val = nums[0] + nums[1]/60 + nums[2]/3600
            
        return val * sign

    # 分別執行轉換
    new_lon = _parse_one(lon)
    new_lat = _parse_one(lat)
    
    return new_lon, new_lat


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
    # 如果重複的時候，不要update的欄位
    not_set_list = ['created', 'tbiaID']
    insert_statement = insert(table.table).values(list(data_iter))
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



# def update_update_version(update_version, rights_holder, current_page=0, note=None, is_finished=False):
#     now = datetime.now() + timedelta(hours=8)
#     conn = psycopg2.connect(**db_settings)
#     if is_finished:
#         query = """
#                 UPDATE update_version SET is_finished = 't', modified = %s WHERE "update_version" = %s and rights_holder = %s;
#                 """
#         cur = conn.cursor()
#         cur.execute(query, (now, update_version, rights_holder))
#     else:
#         query = """
#                 UPDATE update_version SET current_page = %s, note = %s, modified = %s 
#                 WHERE "update_version" = %s and rights_holder = %s;
#                 """
#         cur = conn.cursor()
#         cur.execute(query, (current_page, note, now, update_version, rights_holder))
#     conn.commit()
#     conn.close()


# def insert_new_update_version(update_version, rights_holder):
#     now = datetime.now() + timedelta(hours=8)
#     res = None
#     conn = psycopg2.connect(**db_settings)
#     query = """select * FROM update_version WHERE "update_version" = %s and rights_holder = %s;"""
#     with conn.cursor() as cursor:
#         cursor.execute(query, (update_version, rights_holder))
#         res = cursor.fetchone()
#     if res: # 確認是不是有id 有的話代表這個已經存在
#         return res[2], res[4] # 回傳page & note
#     else:        
#         query = """
#                     INSERT INTO update_version ("current_page","update_version", rights_holder, created, modified) VALUES (0, %s, %s, %s, %s)
#                     """
#         cur = conn.cursor()
#         cur.execute(query, (update_version, rights_holder, now, now))
#         conn.commit()
#         conn.close()
#         return 0, None


def batch_county_lookup(df, raw_lon_col='standardRawLongitude', raw_lat_col='standardRawLatitude',
                        std_lon_col='standardLongitude', std_lat_col='standardLatitude'):
    """批次查詢縣市，取代逐筆 geometry.contains"""
    df['county'] = None
    df['municipality'] = None
    df['rawCounty'] = df.get('rawCounty')
    df['rawMunicipality'] = df.get('rawMunicipality')

    # 優先用 raw 座標，沒有的話用 standard
    lon = df[raw_lon_col].fillna(df[std_lon_col])
    lat = df[raw_lat_col].fillna(df[std_lat_col])
    valid = lon.notna() & lat.notna()

    if not valid.any():
        return df

    points_gdf = gpd.GeoDataFrame(
        df.loc[valid, []],
        geometry=gpd.points_from_xy(lon[valid], lat[valid]),
        crs='EPSG:4326'
    )

    # 陸域 sjoin
    joined = gpd.sjoin(points_gdf, gdf[['COUNTYNAME', 'TOWNNAME', 'geometry']], how='left', predicate='within')
    joined = joined[~joined.index.duplicated(keep='first')]

    # 沒匹配到的查海域
    no_match = joined['COUNTYNAME'].isna()
    if no_match.any():
        ocean_joined = gpd.sjoin(points_gdf.loc[no_match.values], gdf_ocean[['COUNTYO', 'geometry']], how='left', predicate='within')
        ocean_joined = ocean_joined[~ocean_joined.index.duplicated(keep='first')]
        joined.loc[no_match, 'COUNTYNAME'] = ocean_joined['COUNTYO']

    # 根據 is_hidden 分配到 raw 或 standard 欄位
    is_hidden = df.loc[valid, 'is_hidden'] if 'is_hidden' in df.columns else pd.Series(False, index=df.loc[valid].index)

    hidden_mask = valid & is_hidden.reindex(df.index, fill_value=False)
    visible_mask = valid & ~is_hidden.reindex(df.index, fill_value=False)

    df.loc[hidden_mask, 'rawCounty'] = joined.loc[hidden_mask[hidden_mask].index.intersection(joined.index), 'COUNTYNAME']
    df.loc[hidden_mask, 'rawMunicipality'] = joined.loc[hidden_mask[hidden_mask].index.intersection(joined.index), 'TOWNNAME']

    df.loc[visible_mask, 'county'] = joined.loc[visible_mask[visible_mask].index.intersection(joined.index), 'COUNTYNAME']
    df.loc[visible_mask, 'municipality'] = joined.loc[visible_mask[visible_mask].index.intersection(joined.index), 'TOWNNAME']

    return df


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
    # 度分秒座標轉換
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
    # # 確認縣市是否需要屏蔽
    # if is_full_hidden: # 這邊應該就會包含 sensitiveCategory in ['縣市','座標不開放']
    #     grid_data['rawCounty'] = COUNTYNAME
    #     grid_data['rawMunicipality'] = TOWNNAME
    #     grid_data['county'] = None
    #     grid_data['municipality'] = None
    # else:
    #     grid_data['county'] = COUNTYNAME
    #     grid_data['municipality'] = TOWNNAME
    # 縣市查詢改由 batch_county_lookup 批次處理，這裡只放佔位
    if is_full_hidden:
        grid_data['rawCounty'] = None
        grid_data['rawMunicipality'] = None
        grid_data['county'] = None
        grid_data['municipality'] = None
    else:
        grid_data['county'] = None
        grid_data['municipality'] = None
    final_list = []
    for k in geo_keys: # 如果沒有就回會傳 None
        final_list.append(grid_data.get(k))
    return final_list


def clean_html_tags(text):
    # 1. 檢查是否為字串 (處理 NaN 或 float 的情況)
    if not isinstance(text, str):
        return text
    # 2. 移除 <i> 和 </i>
    # 3. 移除前後多餘空白 (.strip())
    return text.replace('<i>', '').replace('</i>', '').strip()

# 沒有模糊化的情況
def create_grid_data_new(verbatimLongitude, verbatimLatitude):
    grid_data = {}
    for field in ['grid_1','grid_5','grid_10','grid_100','grid_1_blurred','grid_5_blurred','grid_10_blurred','grid_100_blurred']:
        grid_data[field] = '-1_-1'
    # 度分秒座標轉換
    if any(ext in str(verbatimLongitude) for ext in ['N', 'S', 'W', 'E']) or any(ext in str(verbatimLatitude) for ext in ['N', 'S', 'W', 'E']):
        lon, lat = convert_to_decimal(verbatimLongitude, verbatimLatitude)
    else:
        lon, lat = verbatimLongitude, verbatimLatitude
    standardLon, standardLat, location_rpt = standardize_coor(lon, lat)
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

# def calculate_data_quality(row):
#     # row = row.to_dict()
#     if (row.get('sourceScientificName') or row.get('originalVernacularName')) and (row.get('standardDate') or (row.get('year') and row.get('month'))) and ((row.get('standardLatitude') and row.get('standardLongitude')) or (row.get('standardRawLatitude') and row.get('standardRawLongitude'))) and (row.get('coordinatesUncertaintyInMeters') or row.get('coordinatePrecision')) and ((row.get('basisOfRecord') in basis_dict.keys() )or (row.get('basisOfRecord') in basis_dict.values())):
#         data_quality = 3
#     elif (row.get('sourceScientificName') or row.get('originalVernacularName')) and (row.get('standardDate') or row.get('year'))  and ((row.get('standardLatitude') and row.get('standardLongitude')) or (row.get('standardRawLatitude') and row.get('standardRawLongitude'))):
#         data_quality = 2
#     else:
#         data_quality = 1
#     return data_quality


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
    row = row.to_dict()
    try:
        coordinatePrecision = float(row.get('coordinatePrecision'))
    except:
        coordinatePrecision = None
    if not coordinatePrecision:
        if row.get('sensitiveCategory') == '輕度':
            coordinatePrecision = 0.01
        elif row.get('sensitiveCategory') == '重度':
            coordinatePrecision = 0.1
    return coordinatePrecision
