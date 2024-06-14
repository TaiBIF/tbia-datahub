import requests
import pandas as pd
from app import portal_db_settings


# url = "http://solr:8983/solr/tbia_records/select?facet.field=datasetName&facet.limit=-1&facet=true&q.op=OR&q=*%3A*&rows=0"
# response = requests.get(url)
# if response.status_code == 200:
#     resp = response.json()
#     dataset_list = resp['facet_counts']['facet_fields']['datasetName'] # 3936
#     dataset_list = [dataset_list[x] for x in range(0, len(dataset_list),2)]
#     # data = resp['response']['docs']


url = "http://solr:8983/solr/tbia_records/select?facet.pivot=locality,recordType&facet.limit=-1&facet=true&q.op=OR&q=*%3A*&rows=0"
response = requests.get(url)
if response.status_code == 200:
    resp = response.json()
    locality_list = resp['facet_counts']['facet_pivot']['locality,recordType'] # 264570
    rows = []
    for l in locality_list:
        # locality = l.get('value')
        record_type = []
        for ll in l.get('pivot'):
            rows.append({'locality': l.get('value'), 'record_type': ll.get('value')})
            # record_type += ll.get('value')
    # locality_list = [locality_list[x].lstrip() for x in range(0, len(locality_list),2)]
    locality = pd.DataFrame(rows)
    locality.to_csv('/solr/tbia_locality_20240510.csv')
    # data = resp['response']['docs']

# import re
# # 清理出現地前綴空格


# import requests
# from scripts.utils import convert_coor_to_grid
# import psycopg2
# from app import db_settings
# from psycopg2.extras import execute_values
# import time


# for d in locality.locality.unique():
#     if re.match(r'\s', d):
#         print(d)
#         url = f"http://solr:8983/solr/tbia_records/select?facet.field=id&facet.limit=-1&facet.mincount=1&facet=true&indent=true&q.op=OR&q=locality%3A%22{d}%22&rows=0&start=0"
#         new_resp = requests.get(url)
#         tbia_ids = new_resp.json()['facet_counts']['facet_fields']['id']
#         tbia_ids = [tbia_ids[x] for x in range(0, len(tbia_ids),2)]
#         # new_resp = new_resp.json()
#         d = d.lstrip()
#         print(d)
#         new_data = []
#         sql = """
#         update records r
#         set
#             "locality" = t."locality"
#         from (values %s) as t("tbiaID", "locality")
#         where r."tbiaID" = t."tbiaID";
#         """
#         for tt in tbia_ids:
#             new_data.append((tt,d))
#         conn = psycopg2.connect(**db_settings)
#         curs = conn.cursor()  # Assuming you already got the connection object
#         execute_values(curs, sql, new_data)
#         conn.commit()
#         conn.close()
