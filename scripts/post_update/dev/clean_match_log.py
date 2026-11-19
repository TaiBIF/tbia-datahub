#!/usr/bin/env python3
"""清理 match_log 中的殘留紀錄：tbiaID 已不存在於 records 的列（紀錄已刪除，match_log 沒跟著刪）。

殘留會讓比對報告的筆數偏高（例：2026-11 TaiBIF match_log 1,106,252 筆、records 1,097,522 筆）。

預設只統計各單位的殘留筆數，不做任何修改；加 --execute 才會備份並刪除，且必須用 --unit 指定單位。
刪除前會先把要刪的 match_log 列完整備份成 csv.gz（/code/rematch_cache/），需要時可用 COPY 匯回。

用法（在 /code 下以模組方式執行）：
  python -m scripts.post_update.dev.clean_match_log                               # 各單位殘留筆數（只讀）
  python -m scripts.post_update.dev.clean_match_log --unit brcas_0                # 只看 TaiBIF（只讀）
  python -m scripts.post_update.dev.clean_match_log --unit brcas_0 --execute      # 備份後刪除

"""
import argparse
import gzip
import sys
from datetime import datetime

from app import engine
from scripts.post_update.dev.match_report import UNITS, unit_key

BACKUP_DIR = '/code/rematch_cache'
DELETE_BATCH = 50000


def main():
    p = argparse.ArgumentParser(description='清理 match_log 中 records 已不存在的殘留紀錄')
    p.add_argument('--unit', nargs='+', default=[], metavar='UNIT',
                   help='只處理這些資料庫，格式 {group}_{info_id}（--execute 時必填）')
    p.add_argument('--execute', action='store_true', help='備份並刪除（未加則只統計）')
    args = p.parse_args()

    known = {unit_key(g, i): rh for rh, g, i in UNITS}
    unknown = set(args.unit) - set(known)
    if unknown:
        sys.exit(f'找不到這些單位：{", ".join(sorted(unknown))}')
    if args.execute and not args.unit:
        sys.exit('--execute 必須搭配 --unit 指定單位，避免一次刪除全部單位')
    if args.execute and 'nmns_0' in args.unit:
        sys.exit('科博（nmns_0）records 疑似被誤刪，確認原因前不清理其 match_log')
    rhs = [known[u] for u in args.unit] or [rh for rh, _, _ in UNITS]

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        # 1. 找出殘留：match_log 有、records 沒有的 tbiaID（records."tbiaID" 有唯一索引）
        print('找出殘留紀錄（掃描 match_log）...', flush=True)
        cur.execute('DROP TABLE IF EXISTS _ml_orphan')
        cur.execute('''
            CREATE TEMP TABLE _ml_orphan AS
            SELECT row_number() OVER () AS n, m."tbiaID", m.rights_holder
            FROM match_log m
            WHERE m.rights_holder = ANY(%s)
              AND NOT EXISTS (SELECT 1 FROM records r WHERE r."tbiaID" = m."tbiaID")
        ''', (rhs,))
        cur.execute('CREATE INDEX ON _ml_orphan (n)')
        cur.execute('SELECT rights_holder, count(*) FROM _ml_orphan GROUP BY 1 ORDER BY 2 DESC')
        rows = cur.fetchall()
        total = sum(c for _, c in rows)
        for rh, c in rows:
            print(f'{c:>10,}  {rh}')
        print(f'{total:>10,}  合計')

        if not args.execute:
            print('（只統計，未修改任何資料；確認後加 --execute 執行）')
            raw.rollback()
            return
        if not total:
            print('沒有殘留紀錄')
            return

        # 2. 備份要刪除的 match_log 列
        path = f'{BACKUP_DIR}/match_log_orphans_{datetime.now():%Y%m%d_%H%M}.csv.gz'
        print(f'備份到 {path} ...', flush=True)
        with gzip.open(path, 'wt', encoding='utf-8', newline='') as f:
            cur.copy_expert('''
                COPY (SELECT m.* FROM match_log m
                      WHERE m."tbiaID" IN (SELECT "tbiaID" FROM _ml_orphan))
                TO STDOUT WITH (FORMAT csv, HEADER)
            ''', f)

        # 3. 分批刪除（每批 commit；中斷後重跑會重新找殘留，已刪的不會重複計算）
        deleted = 0
        for lo in range(1, total + 1, DELETE_BATCH):
            cur.execute('''
                DELETE FROM match_log m USING _ml_orphan o
                WHERE m."tbiaID" = o."tbiaID" AND o.n BETWEEN %s AND %s
            ''', (lo, lo + DELETE_BATCH - 1))
            deleted += cur.rowcount
            raw.commit()
            print(f'  已刪除 {deleted:,} / {total:,}', flush=True)
        print(f'完成：刪除 {deleted:,} 筆，備份 {path}')
        print('建議接著重跑該單位的 match_report（--unit ... --no-delta），並 ANALYZE match_log')
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


if __name__ == '__main__':
    main()