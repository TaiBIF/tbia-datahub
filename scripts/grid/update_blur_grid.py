import requests
from scripts.utils import convert_coor_to_grid
import psycopg2
from app import db_settings
from psycopg2.extras import execute_values
import time


has_more_data = True
limit = 1000
offset = 0
while has_more_data:
    s = time.time()
    response = requests.get(f'http://solr:8983/solr/tbia_records/select?fl=id%2CstandardLatitude%2CstandardLongitude&indent=true&q.op=OR&q=location_rpt%3A*&rows={limit}&sort=id%20desc&start={offset}')
    if response.status_code == 200:
        resp = response.json()
        data = resp['response']['docs']
        if len(data):
            new_data = []
            for d in data:
                tmp_dict = {'id': d['id']}
                if d.get('standardLatitude') and d.get('standardLongitude'):
                    standardLon = d['standardLongitude'][0]
                    standardLat = d['standardLatitude'][0]
                    if '.' in str(standardLon) and '.' in str(standardLat):
                        float_len = min(len(str(standardLon).split('.')[-1]),len(str(standardLat).split('.')[-1]))
                    else:
                        float_len = 0
                    # 如果小數點超過兩位
                    if float_len >= 2:
                        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.01)
                        tmp_dict['grid_1_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
                    else:
                        tmp_dict['grid_1_blurred'] = None
                    # 如果小數點超過一位
                    if float_len >= 1:
                        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.05)
                        tmp_dict['grid_5_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
                        # 如果小數點超過一位
                        grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 0.1)
                        tmp_dict['grid_10_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
                    else:
                        tmp_dict['grid_5_blurred'] = None
                        tmp_dict['grid_10_blurred'] = None
                    grid_x, grid_y = convert_coor_to_grid(standardLon, standardLat, 1)
                    tmp_dict['grid_100_blurred'] = str(int(grid_x)) + '_' + str(int(grid_y))
                    new_data.append((tmp_dict['id'], tmp_dict['grid_1_blurred'], tmp_dict['grid_5_blurred'], tmp_dict['grid_10_blurred'], tmp_dict['grid_100_blurred']))
            if len(new_data):
                # 更新資料
                sql = """
                    update records r
                    set
                        grid_1_blurred = t.grid_1_blurred,
                        grid_5_blurred = t.grid_5_blurred,
                        grid_10_blurred = t.grid_10_blurred,
                        grid_100_blurred = t.grid_100_blurred
                    from (values %s) as t("tbiaID" ,grid_1_blurred, grid_5_blurred, grid_10_blurred, grid_100_blurred)
                    where r."tbiaID" = t."tbiaID";
                """
                conn = psycopg2.connect(**db_settings)
                curs = conn.cursor()  # Assuming you already got the connection object
                execute_values(curs, sql, new_data)
            print(offset, time.time()-s)
            offset += limit
        else:
            has_more_data = False
