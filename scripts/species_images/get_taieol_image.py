
import psycopg2
from app import db_settings
import pandas as pd
import requests
import json


def update_species_images(taxon_name_id, namecode, taieol_id, images):
    conn = psycopg2.connect(**db_settings)
    query = """
            INSERT INTO species_images ("taxon_name_id", "namecode", "taieol_id", "images", "modified")
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT ("taxon_name_id","namecode") 
            DO UPDATE SET taieol_id = %s, images = %s, modified = NOW();
            """
    cur = conn.cursor()
    cur.execute(query, (taxon_name_id, namecode, taieol_id, images, taieol_id, images))
    conn.commit()
    conn.close()


df = pd.read_csv('/bucket/taicol-web-namecode-2023-11-7.csv')

for i in df.index:
    if i % 100 == 0:
        print(i)
    images = []
    row = df.loc[i]
    taxon_name_id = str(row.taxon_name_id)
    namecode = row.namecode
    taieol_id = None
    # 圖片
    url = 'https://data.taieol.tw/eol/endpoint/image/species/{}'.format(namecode)
    r = requests.get(url)
    img = r.json()
    if img:
        for ii in img:
            images += [{'author':ii['author'], 'src':ii['image_big'], 'provider':ii['provider']}]
    images = json.dumps(images)
    url = 'https://data.taieol.tw/eol/endpoint/taxondesc/species/{}'.format(namecode)
    r = requests.get(url)
    tid = r.json()
    if tid:
        taieol_id = tid['tid']
    update_species_images(taxon_name_id, namecode, taieol_id, images)
