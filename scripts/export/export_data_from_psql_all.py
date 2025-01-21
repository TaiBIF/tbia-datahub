# 重新比對的時候要抓回原始檔

import pandas as pd
from app import db
import pandas as pd
import sqlalchemy as sa
from scripts.utils import get_taxon_df, db_settings
import time

total_count = 0

limit = 10000
offset = 0
min_id = 0
has_more_data = True
while has_more_data:
    s = time.time()
    results = []
    with db.begin() as conn:
        qry = sa.text("""select * from records  
                        where id > {} order by id limit {}  """.format(min_id, limit)) 
        resultset = conn.execute(qry)
        results = resultset.mappings().all()
    print(time.time()-s, offset, min_id)
    if len(results):
        total_count += len(results)
        df = pd.DataFrame(results)
        # 下一次query最小的id
        min_id = df.id.max()
        df = df.drop(columns=['id'])
        df = df.rename(columns={'tbiaID': 'id'})
        if len(df[df.taxonID.notnull()]):
            taxon = get_taxon_df(taxon_ids=df[df.taxonID.notnull()].taxonID.unique())
            # taxonID
            if len(taxon):
                final_df = df.merge(taxon,on='taxonID',how='left')
            else:
                final_df = df
        else:
            final_df = df
        if len(results) != len(final_df):
            print('error', min_id)
        final_df = final_df.rename(columns={'originalVernacularName': 'originalScientificName'})
        final_df.to_csv(f'/solr/csvs/export/export_{offset}.csv', index=None)
        offset += limit
    if len(results) < limit:
        has_more_data = False

print('total_count', total_count)