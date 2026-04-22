# 重新比對的時候要抓回原始檔

import pandas as pd
from app import db
import pandas as pd
import sqlalchemy as sa
from scripts.utils import get_taxon_df
import time

taxon_cache = {}
total_count = 0
limit = 50000
offset = 0
min_id = 0
has_more_data = True

while has_more_data:
    s = time.time()

    df = pd.read_sql(
        sa.text("SELECT * FROM records WHERE id > :min_id ORDER BY id LIMIT :limit"),
        db, params={"min_id": min_id, "limit": limit}
    )

    print(time.time() - s, offset, min_id)

    if len(df):
        total_count += len(df)
        min_id = df.id.max()
        df = df.drop(columns=['id'])
        df = df.rename(columns={'tbiaID': 'id'})

        if len(df[df.taxonID.notnull()]):
            unique_ids = df[df.taxonID.notnull()].taxonID.unique()
            missing_ids = [tid for tid in unique_ids if tid not in taxon_cache]
            if missing_ids:
                new_taxon = get_taxon_df(taxon_ids=missing_ids)
                for _, row in new_taxon.iterrows():
                    taxon_cache[row['taxonID']] = row.to_dict()
            cached = [taxon_cache[tid] for tid in unique_ids if tid in taxon_cache]
            taxon = pd.DataFrame(cached) if cached else pd.DataFrame()

            if len(taxon):
                final_df = df.merge(taxon, on='taxonID', how='left')
            else:
                final_df = df
        else:
            final_df = df

        if len(df) != len(final_df):
            print('error', min_id)

        final_df = final_df.rename(columns={'originalVernacularName': 'originalScientificName'})
        final_df.to_csv(f'/solr/csvs/export/export_{offset}.csv', index=None)
        offset += limit

    if len(df) < limit:
        has_more_data = False

print('total_count', total_count)