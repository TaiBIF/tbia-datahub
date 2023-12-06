import time
from app import db
import sqlalchemy as sa

has_more_data = True
min_id = 1
offset = 0
while has_more_data:
    s = time.time()
    with db.begin() as conn:
        qry = sa.text(f"""
        with base as (select id,grid_1,grid_5,grid_10,grid_100 from records where id > {min_id} order by id limit 1000)
        UPDATE records 
            SET
                grid_1_blurred = b.grid_1,
                grid_5_blurred =  b.grid_5,
                grid_10_blurred =  b.grid_10,
                grid_100_blurred =  b.grid_100
            FROM base b 
            WHERE records.id = b.id 
            RETURNING records.id;
        """)
        resultset = conn.execute(qry)
        results = resultset.mappings().all()
        if len(results):
            offset += len(results)
            print(offset, min_id, time.time()-s)
            min_id = results[-1]['id']
        else:
            has_more_data = False


print('done!', offset)