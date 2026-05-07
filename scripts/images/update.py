# 更新生命大百科照片

import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from app import db_settings

load_dotenv(override=True)

TOKEN = os.getenv('TAIEOL_TOKEN')
SOLR_URL = 'http://solr:8983/solr/taxa/select'
TAIEOL_URL = 'https://data.taieol.tw/api/v2/taieol_object/taxon_id/{}?token={}'

SOLR_BATCH = 500       # Solr 一次拉的筆數
MAX_WORKERS = 20       # TaiEOL 併發數
HTTP_TIMEOUT = 30

# 共用 Session：keep-alive + 連線池 + 5xx 自動重試
session = requests.Session()
session.headers.update({'user-agent': 'TaiCOL'})
retry = Retry(total=3, backoff_factor=0.5,
              status_forcelist=[500, 502, 503, 504],
              allowed_methods=['GET'])
adapter = HTTPAdapter(max_retries=retry,
                      pool_connections=MAX_WORKERS,
                      pool_maxsize=MAX_WORKERS)
session.mount('http://', adapter)
session.mount('https://', adapter)


def fetch_taieol(taxon_id):
    """抓單一 taxon 的 TaiEOL 資料，回傳 (taxon_id, taieol_id, images_json)。"""
    images = []
    taieol_id = None
    try:
        r = session.get(TAIEOL_URL.format(taxon_id, TOKEN), timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return taxon_id, None, json.dumps(images)
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        print(f'[warn] taxon {taxon_id} fetch failed: {e}')
        return taxon_id, None, json.dumps(images)

    try:
        taieol_id = data['meta']['q']['scientificNameID']
    except (KeyError, TypeError):
        pass

    for oo in data.get('data') or []:
        for ii in oo.get('associatedMedia') or []:
            images.append({
                'author': (ii.get('authors') or '').replace('作者：', ''),
                'src': ii.get('url'),
                'provider': oo.get('sourceName'),
                'permalink': oo.get('permanentLink'),
                'license': ii.get('licence'),
            })
    return taxon_id, taieol_id, json.dumps(images)


def upsert_batch(cur, rows):
    if not rows:
        return
    sql = """
        INSERT INTO species_images (taxon_id, taieol_id, images, modified)
        VALUES %s
        ON CONFLICT (taxon_id) DO UPDATE
        SET taieol_id = EXCLUDED.taieol_id,
            images    = EXCLUDED.images,
            modified  = NOW();
    """
    execute_values(cur, sql, rows, template='(%s, %s, %s, NOW())')


def iter_solr_ids():
    """用 cursorMark 走訪 Solr 全部 taxon id，每次 yield 一個 batch。"""
    cursor = '*'
    while True:
        params = {
            'q': '*:*',
            'fl': 'id',
            'rows': SOLR_BATCH,
            'sort': 'id asc',     # cursorMark 必須有 unique sort
            'cursorMark': cursor,
            'wt': 'json',
        }
        r = session.get(SOLR_URL, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        resp = r.json()
        docs = resp['response']['docs']
        if docs:
            yield [d['id'] for d in docs]
        next_cursor = resp.get('nextCursorMark')
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor


def main():
    conn = psycopg2.connect(**db_settings)
    cur = conn.cursor()
    total = 0
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for ids in iter_solr_ids():
                results = list(pool.map(fetch_taieol, ids))
                upsert_batch(cur, results)
                conn.commit()
                total += len(results)
                print(f'processed {total}')
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()