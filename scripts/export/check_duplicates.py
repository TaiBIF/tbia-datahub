import pandas as pd
from app import db
import pandas as pd
import sqlalchemy as sa
import time

rights_holder_list = {
    0: 'GBIF',
    1: '中央研究院生物多樣性中心動物標本館',
    2: '中央研究院生物多樣性中心植物標本資料庫',
    3: '台灣生物多樣性網絡 TBN',
    4: '國立臺灣博物館典藏',
    5: '林業試驗所昆蟲標本館',
    6: '林業試驗所植物標本資料庫',
    7: '河川環境資料庫',
    8: '濕地環境資料庫',
    9: '生態調查資料庫系統',
    10: '臺灣國家公園生物多樣性資料庫',
    11: '臺灣生物多樣性資訊機構 TaiBIF',
    12: '海洋保育資料倉儲系統',
    13: '科博典藏 (NMNS Collection)',
    14: '臺灣魚類資料庫',
    15: '國家海洋資料庫及共享平台',
    16: '集水區友善環境生態資料庫',
    17: '中油生態地圖',
    18: '作物種原資訊系統',
    19: '國立海洋生物博物館生物典藏管理系統',
}

# 輸入要處理的編號，例如 "0,3,5" 或 "0"
import os
selected_indices = [int(x.strip()) for x in os.environ['SELECTED'].split(',')]

for idx in selected_indices:
    rh = rights_holder_list[idx]

    s = time.time()
    
    # 1. 找 occurrenceID 重複
    with db.begin() as conn:
        qry = sa.text("""
            SELECT "occurrenceID" FROM records 
            WHERE "rightsHolder" = :rh AND "occurrenceID" IS NOT NULL AND "occurrenceID" != ''
            GROUP BY "occurrenceID" HAVING count(distinct("tbiaID")) > 1;
        """)
        dup_occ = [r['occurrenceID'] for r in conn.execute(qry, {'rh': rh}).mappings().all()]
    
    # 2. 找 catalogNumber 重複
    with db.begin() as conn:
        qry = sa.text("""
            SELECT "catalogNumber" FROM records 
            WHERE "rightsHolder" = :rh AND "catalogNumber" IS NOT NULL AND "catalogNumber" != ''
            GROUP BY "catalogNumber" HAVING count(distinct("tbiaID")) > 1;
        """)
        dup_cat = [r['catalogNumber'] for r in conn.execute(qry, {'rh': rh}).mappings().all()]
    
    print(f"{rh}: {len(dup_occ)} dup occurrenceID, {len(dup_cat)} dup catalogNumber, {time.time()-s:.1f}s")
    
    if dup_occ:
        pd.DataFrame(dup_occ, columns=['occurrenceID']).to_csv(f'removed_dup_occ_{rh}.csv', index=None)
    if dup_cat:
        pd.DataFrame(dup_cat, columns=['catalogNumber']).to_csv(f'removed_dup_cat_{rh}.csv', index=None)

    # 3. 刪除 occurrenceID 重複
    for occ_id in dup_occ:
        with db.begin() as conn:
            conn.execute(sa.text("""
                WITH moved_rows AS (
                    DELETE FROM records a USING records b
                    WHERE a."occurrenceID" = :occ_id 
                      AND a."occurrenceID" = b."occurrenceID"
                      AND a."rightsHolder" = b."rightsHolder"
                      AND a.id < b.id
                    RETURNING a."tbiaID"
                )
                DELETE FROM match_log WHERE "tbiaID" IN (SELECT "tbiaID" FROM moved_rows)
            """), {'occ_id': occ_id})
    
    # 4. 刪除 catalogNumber 重複
    for cat_num in dup_cat:
        with db.begin() as conn:
            conn.execute(sa.text("""
                WITH moved_rows AS (
                    DELETE FROM records a USING records b
                    WHERE a."catalogNumber" = :cat_num 
                      AND a."catalogNumber" = b."catalogNumber"
                      AND a."rightsHolder" = b."rightsHolder"
                      AND a.id < b.id
                    RETURNING a."tbiaID"
                )
                DELETE FROM match_log WHERE "tbiaID" IN (SELECT "tbiaID" FROM moved_rows)
            """), {'cat_num': cat_num})

# groups = []

# for group in groups:
#     s = time.time()
#     results = []
#     with db.begin() as conn:
#         qry = sa.text("""select "occurrenceID" from records where "group" = '{}' group by "occurrenceID"  having count(distinct("tbiaID")) >1;""".format(group))
#         resultset = conn.execute(qry)
#         results = resultset.mappings().all()
#     print(time.time()-s) #, offset, min_id)        
#     if len(results):
#         print(len(results))
#         df = pd.DataFrame(results, columns=['occurrenceID'])
#         df.to_csv('removed_duplicates_{}.csv'.format(group), index=None)
#         for i in df.index:
#             row = df.iloc[i]            
#             with db.begin() as conn:
#                 qry = sa.text("""
#                 WITH moved_rows AS (
#                     delete 
#                     from records a 
#                     using records b
#                     where a."occurrenceID" = '{}' and a."occurrenceID" = b."occurrenceID" and a."datasetName" = b."datasetName" and a."rightsHolder" = b."rightsHolder"
#                     and a.id < b.id
#                     RETURNING a."tbiaID"
#                 )
#                     DELETE FROM match_log 
#                     WHERE "tbiaID" IN (select "tbiaID" from moved_rows)
#                     """.format(row.occurrenceID))
#                 resultset = conn.execute(qry)
