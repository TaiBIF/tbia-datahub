import requests
from scripts.utils import convert_coor_to_grid
import psycopg2
from app import db_settings
from psycopg2.extras import execute_values
import time

import datetime

# date_formats = ['%Y0%m0%d']

def convert_date(date):
    formatted_date = None
    try:
        formatted_date = datetime.strptime(date, '%Y0%m0%d')
    except:
        pass
    return formatted_date



response = requests.get(f'http://solr:8983/solr/tbia_records/select?fl=eventDate&facet.field=eventDate&facet.limit=-1&facet.mincount=1&facet=true&indent=true&q.op=OR&q=standardDate%3A%5B2023-12-01T00%3A00%3A00Z%20TO%20*%5D&rows=0&start=0')
if response.status_code == 200:
    resp = response.json()
    data = resp['facet_counts']['facet_fields']['eventDate']
    data = [data[x] for x in range(0, len(data),2)]
    for d in data:
        s = time.time()
        new_date = convert_date(d)
        # 找出所有相對應的tbiaID並更新
        url = f"http://solr:8983/solr/tbia_records/select?facet.field=id&facet.limit=-1&facet.mincount=1&facet=true&indent=true&q.op=OR&q=eventDate%3A%22{d}%22&rows=0&start=0"
        new_resp = requests.get(url)
        tbia_ids = new_resp.json()['facet_counts']['facet_fields']['id']
        tbia_ids = [tbia_ids[x] for x in range(0, len(tbia_ids),2)]
        # new_resp = new_resp.json()
        new_data = []
        if not new_date:
            sql = """
            update records r
            set
                "standardDate" = NULL
            from (values %s) as t("tbiaID")
            where r."tbiaID" = t."tbiaID";
            """
            for tt in tbia_ids:
                new_data.append((tt,))
        else:
            sql = """
                update records r
                set
                    "standardDate" = t."standardDate"
                from (values %s) as t("tbiaID", "standardDate")
                where r."tbiaID" = t."tbiaID";
            """
            for tt in tbia_ids:
                new_data.append((tt, new_date))
        conn = psycopg2.connect(**db_settings)
        curs = conn.cursor()  # Assuming you already got the connection object
        execute_values(curs, sql, new_data)
        conn.commit()
        conn.close()
        print(d, time.time()-s)


print('done!')