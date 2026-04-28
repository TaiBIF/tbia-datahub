import pandas as pd
import requests
import json
import numpy as np

def fetch_taxon_df(taxon_ids, solr_url='http://solr:8983/solr/taxa/select', chunk_size=500):
    """依 taxonID 列表從 Solr 撈回對應 taxon 資料，回傳已清洗的 DataFrame。"""
    taxon_ids = [tid for tid in set(taxon_ids) if tid]
    if not taxon_ids:
        return pd.DataFrame()

    docs = []
    for i in range(0, len(taxon_ids), chunk_size):
        chunk = taxon_ids[i:i+chunk_size]
        query = {
            'query': '{!terms f=id}' + ','.join(chunk),
            'limit': len(chunk),
        }
        r = requests.post(
            solr_url,
            data=json.dumps(query),
            headers={'content-type': 'application/json'},
        )
        if r.status_code == 200:
            docs += r.json()['response']['docs']
        else:
            print(f'[fetch_taxon_df] solr error {r.status_code} at chunk {i}')

    if not docs:
        return pd.DataFrame()

    taxon_df = pd.DataFrame(docs)
    taxon_df = taxon_df.rename(columns={'id': 'taxonID'})
    taxon_df = taxon_df[taxon_df.columns.drop(list(taxon_df.filter(regex='_taxonID')))]
    taxon_df = taxon_df.drop(columns=['taxon_name_id', '_version_'], errors='ignore')
    taxon_df = taxon_df.replace({np.nan: None})
    return taxon_df


def export_records_with_taxon(df, output_path):
    """把 records df 與 taxon 資料合併後輸出 CSV。"""
    taxon_df = fetch_taxon_df(df['taxonID'].dropna().tolist())

    export_df = df.rename(columns={'tbiaID': 'id'})
    if len(taxon_df):
        export_df = export_df.merge(taxon_df, on='taxonID', how='left')
        if len(export_df) != len(df):
            print(f'[export_records_with_taxon] merge row count mismatch: {len(df)} -> {len(export_df)}')
    export_df = export_df.rename(columns={'originalVernacularName': 'originalScientificName'})
    export_df.to_csv(output_path, index=None)
    # return export_df