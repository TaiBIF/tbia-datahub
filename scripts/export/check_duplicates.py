import pandas as pd
from app import db
import pandas as pd
import sqlalchemy as sa
import time



groups = []

for group in groups:
    s = time.time()
    results = []
    with db.begin() as conn:
        qry = sa.text("""select "occurrenceID" from records where "group" = '{}' group by "occurrenceID"  having count(distinct("tbiaID")) >1;""".format(group))
        resultset = conn.execute(qry)
        results = resultset.mappings().all()
    print(time.time()-s) #, offset, min_id)        
    if len(results):
        print(len(results))
        df = pd.DataFrame(results, columns=['occurrenceID'])
        df.to_csv('removed_duplicates_{}.csv'.format(group), index=None)
        for i in df.index:
            row = df.iloc[i]            
            with db.begin() as conn:
                qry = sa.text("""
                WITH moved_rows AS (
                    delete 
                    from records a 
                    using records b
                    where a."occurrenceID" = '{}' and a."occurrenceID" = b."occurrenceID" and a."datasetName" = b."datasetName" and a."rightsHolder" = b."rightsHolder"
                    and a.id < b.id
                    RETURNING a."tbiaID"
                )
                    DELETE FROM match_log 
                    WHERE "tbiaID" IN (select "tbiaID" from moved_rows)
                    """.format(row.occurrenceID))
                resultset = conn.execute(qry)
