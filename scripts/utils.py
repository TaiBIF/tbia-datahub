
from datetime import datetime, timedelta
import numpy as np
import bisect
import re
from app import portal_db_settings, db, db_settings
import psycopg2
import sqlalchemy as sa
import requests
from sqlalchemy.dialects.postgresql import insert

issue_map = {
    1: 'higherrank',
    2: 'none',
    3: 'fuzzy',
    4: 'multiple'
}


date_formats = ['%Y/%m/%d','%Y%m%d','%Y-%m-%d','%Y/%m/%d %H:%M:%S','%Y-%m-%d %H:%M',
                '%Y/%m/%d %H:%M','%Y-%m-%d %H:%M:%S','%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d %p %I:%M:%S', '%Y/%m/%d %H', '%Y-%m-%d %H']

def convert_date(date):
    formatted_date = None
    if date != '' and date is not None:
        date = str(date)
        date = date.replace('上午','AM').replace('下午','PM')
        for ff in date_formats:
            try:
                formatted_date = datetime.strptime(date, ff)
                return formatted_date
            except:
                formatted_date = None
        if not formatted_date:
            try: 
                date = date.split('T')[0]
                formatted_date = datetime.strptime(date, '%Y-%m-%d')
                return formatted_date
            except:
                formatted_date = None        
        if not formatted_date:
            try:
                formatted_date = datetime.fromtimestamp(int(date))
                return formatted_date
            except:
                formatted_date = None
    return formatted_date


def convert_year_month_day():
    # YYYY-00-00
    # YYYY-MM-00
    # MM-DD
    # MM-0
    # 1968-04-26/27
    # YYYY-MM-DD/DD
    pass





def convert_coor_to_grid(x, y, grid):
    list_x = np.arange(-180, 180+grid, grid)
    list_y = np.arange(-90, 90+grid, grid)
    grid_x = bisect.bisect(list_x, x)-1
    grid_y = bisect.bisect(list_y, y)-1
    return grid_x, grid_y



# N, S, W, E
def convert_to_decimal(lon, lat):
    try:
        deg, minutes, seconds =  re.split('[°\']', lat)
        seconds = seconds[:-1]
        direction = seconds[-1]
        lat = (float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in ['W', 'S'] else 1)
    except:
        lat = None
    try:
        deg, minutes, seconds =  re.split('[°\']', lon)
        seconds = seconds[:-1]
        direction = seconds[-1]
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
        # else:
        #     quantity = None
    except:
        pass
        # quantity = None
    return quantity



basis_dict = {
    "人為觀測": "HumanObservation",
    "出現紀錄": "Occurrence",
    "機器觀測": "MachineObservation",
    "保存標本": "PreservedSpecimen",
    "組織樣本": "MaterialSample",
    "活體標本": "LivingSpecimen",
    "化石標本": "FossilSpecimen",
    "": "MaterialCitation"
}

def control_basis_of_record(basisOfRecord):
    if basisOfRecord in basis_dict.keys():
        basisOfRecord = basis_dict[basisOfRecord]
    return basisOfRecord


# ds_name = df[['datasetName','recordType']].drop_duplicates().to_dict(orient='records')
def update_dataset_key(ds_name, rights_holder):
    # 存在portal的資料庫中
    conn = psycopg2.connect(**portal_db_settings)
    query = """
            INSERT INTO data_datasetkey ("rights_holder", "name", "record_type", "deprecated")
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ("name", "record_type","rights_holder") DO UPDATE SET deprecated = %s;
            """
    for r in ds_name:
        cur = conn.cursor()
        cur.execute(query, (rights_holder,r['datasetName'],r['recordType'],False,False))
        conn.commit()
    conn.close()




def matchlog_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    set_list = ['sourceScientificName','is_matched','taxonID','parentTaxonID','match_stage',
                'stage_1','stage_2','stage_3','stage_4','stage_5','modified']
    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"tbiaID_unique",
        # 如果重複的時候，需要update的欄位
        set_={c.key: c for c in insert_statement.excluded if c.key in set_list},
    )
    conn.execute(upsert_statement)


def update_match_log(match_log, now):
    match_log['is_matched'] = False
    match_log.loc[match_log.taxonID.notnull(),'is_matched'] = True
    match_log = match_log.replace({np.nan: None})
    match_log['match_stage'] = match_log['match_stage'].apply(lambda x: int(x) if x or x == 0 else None)
    match_log['stage_1'] = match_log['stage_1'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_2'] = match_log['stage_2'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_3'] = match_log['stage_3'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_4'] = match_log['stage_4'].apply(lambda x: issue_map[x] if x else x)
    match_log['stage_5'] = match_log['stage_5'].apply(lambda x: issue_map[x] if x else x)
    match_log['created'] = now
    match_log['modified'] = now
    match_log = match_log.rename(columns={'id': 'tbiaID','rightsHolder':'rights_holder'})
    match_log.to_sql('match_log', db, # schema='my_schema',
              if_exists='append',
              index=False,
              method=matchlog_upsert)  
    return match_log


def get_records(rights_holder, min_id, limit=10000):
    with db.begin() as conn:
        qry = sa.text("""select * from records  
                        where "rightsHolder" = '{}' AND id > {} order by id limit {}  """.format(rights_holder, min_id, limit)) 
        resultset = conn.execute(qry)
        results = resultset.mappings().all()
        return results
    

def get_gbif_id(gbifDatasetID, ocurrenceID):
    gbif_url = f"https://api.gbif.org/v1/occurrence/{gbifDatasetID}/{ocurrenceID}"
    gbif_resp = requests.get(gbif_url)
    gbifID = None
    if gbif_resp.status_code == 200:
        gbif_res = gbif_resp.json()
        # occurrenceID = gbif_res.get('occurrenceID')
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

import subprocess

def zip_match_log(group, info_id):
    zip_file_path = f'/portal/media/match_log/{group}_{info_id}_match_log.zip'
    csv_file_path = f'{group}_{info_id}*.csv'
    commands = f"cd /portal/media/match_log/; zip -j {zip_file_path} {csv_file_path}; rm {csv_file_path}"
    process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 等待檔案完成
    a = process.communicate()
    return a


def delete_records(rights_holder,group):
    # 刪除is_deleted的records & match_log
    query = """
                WITH moved_rows AS (
                    DELETE FROM records a
                    WHERE a.is_deleted = 't' and a."rightsHolder" = '{}' and a."group" = '{}'
                    RETURNING a."tbiaID", a."occurrenceID", a."rightsHolder", a."group"
                ), delete_match_log AS (
                    DELETE FROM match_log 
                    WHERE "tbiaID" IN (select "tbiaID" from moved_rows)
                )
                INSERT INTO deleted_records ("tbiaID", "occurrenceID", "rights_holder", "group", "deleted")
                SELECT *, NOW() as deleted FROM moved_rows;
                """.format(rights_holder, group)
    conn = psycopg2.connect(**db_settings)
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        conn.commit()
    return execute_line

