
import psycopg2
from app import db_settings
import pandas as pd
import requests
import json

from dotenv import load_dotenv
import os
load_dotenv(override=True)

def update_species_images(taxon_id, taieol_id, images):
    conn = psycopg2.connect(**db_settings)
    query = """
            INSERT INTO species_images ("taxon_id", "taieol_id", "images", "modified")
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT ("taxon_id") 
            DO UPDATE SET taieol_id = %s, images = %s, modified = NOW();
            """
    cur = conn.cursor()
    cur.execute(query, (taxon_id, taieol_id, images, taieol_id, images))
    conn.commit()
    conn.close()


has_more_data = True
token = os.getenv('TAIEOL_TOKEN')

start = 0
while has_more_data:
    print(start)
    response = requests.get('http://solr:8983/solr/taxa/select?&q=*%3A*&fl=id&start={}'.format(start))
    if response.status_code == 200:
        resp = response.json()
        for dd in resp['response']['docs']:
            url = 'https://data.taieol.tw/api/v2/taieol_object/taxon_id/{}?token={}'.format(dd.get('id'), token)
            taieol_resp = requests.get(url, headers={'user-agent':"TaiCOL"})
            images = []
            taieol_id = None
            if taieol_resp.status_code == 200:
                try:
                    taieol_id = taieol_resp.json()['meta']['q']['scientificNameID']
                except:
                    pass
                obj = taieol_resp.json()['data']
                for oo in obj:
                    img = oo.get('associatedMedia')
                    for ii in img:
                        foto = {'author': ii['authors'].replace('作者：',''), 'src': ii['url'], 
                                'provider': oo['sourceName'], 'permalink': oo['permanentLink'], 
                                'license': ii['licence']}
                        images.append(foto)
            update_species_images(dd.get('id'), taieol_id, json.dumps(images))
        if len(resp['response']['docs']) < 10:
            has_more_data = False
        start += 10
    
