import requests

SOLR_BASE = 'http://solr:8983/solr'


def solr_reimport_from_csv(core, csv_path, base=SOLR_BASE):
    """刪除指定 Solr core 內所有文件，再從 CSV 重匯。"""
    # 1. delete all
    r = requests.post(
        f'{base}/{core}/update',
        params={'commit': 'true'},
        headers={'Content-Type': 'text/xml'},
        data='<delete><query>*:*</query></delete>',
    )
    r.raise_for_status()
    # 2. import csv
    with open(csv_path, 'rb') as f:
        r = requests.post(
            f'{base}/{core}/update',
            params={'commit': 'true'},
            headers={'Content-Type': 'text/csv'},
            data=f,
        )
    r.raise_for_status()
    return r.json()


def solr_import_csv(core, csv_path, base=SOLR_BASE):
    """從 CSV 匯入，Solr 依 uniqueKey 覆寫既有文件，不刪除、不影響其他文件。"""
    with open(csv_path, 'rb') as f:
        r = requests.post(
            f'{base}/{core}/update',
            params={'commit': 'true'},
            headers={'Content-Type': 'text/csv'},
            data=f,
        )
    r.raise_for_status()