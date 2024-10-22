from app import db
import sqlalchemy as sa
import time 

s = time.time()
with db.begin() as conn:
    qry = sa.text("""select count(distinct("tbiaID")) as tbia_count,count(distinct("occurrenceID")) as occ_count, count(distinct("catalogNumber")) as cata_count, "rightsHolder" from records group by "rightsHolder" """ )
    resultset = conn.execute(qry)
    results = resultset.mappings().all()
    for r in results:
        print(r)

print(time.time()-s)



s = time.time()
with db.begin() as conn:
    qry = sa.text("""select count(distinct("tbiaID")) as tbia_count,count(distinct("occurrenceID")) as occ_count, count(distinct("catalogNumber")) as cata_count, "rights_holder" from match_log group by "rights_holder" """ )
    resultset = conn.execute(qry)
    results = resultset.mappings().all()
    for r in results:
        print(r)

print(time.time()-s)