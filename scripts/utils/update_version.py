# 紀錄update相關的參數

import atexit
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import requests
from scripts.utils.records import OptimizedRecordsProcessor
from scripts.utils.match import OptimizedMatchLogProcessor
from scripts.utils.deduplicates import DedupTracker
from app import db, db_settings
import psycopg2
from scripts.utils.progress import timed


@dataclass
class UpdateSession:
    update_version: int
    current_page: int
    note: Optional[str]
    now: datetime
    records_processor: 'OptimizedRecordsProcessor'
    matchlog_processor: 'OptimizedMatchLogProcessor'
    dedup_tracker: 'DedupTracker'


def insert_new_update_version(update_version, rights_holder):
    now = datetime.now() + timedelta(hours=8)
    with psycopg2.connect(**db_settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT current_page, note FROM update_version '
                'WHERE update_version = %s AND rights_holder = %s',
                (update_version, rights_holder),
            )
            if res := cursor.fetchone():
                return res
            cursor.execute(
                'INSERT INTO update_version '
                '(current_page, update_version, rights_holder, created, modified) '
                'VALUES (0, %s, %s, %s, %s)',
                (update_version, rights_holder, now, now),
            )
            return 0, None


def update_update_version(update_version, rights_holder, current_page=0, note=None,
                          is_finished=False, total_count=None):
    """更新 update_version 紀錄。

    Args:
        total_count: 累積成功寫入 records 的筆數；None 表示不更新此欄位。
    """
    now = datetime.now() + timedelta(hours=8)
    with psycopg2.connect(**db_settings) as conn:
        with conn.cursor() as cursor:
            # 動態組合 SET 子句，避免重複 if/else
            set_clauses = ['modified = %s']
            params = [now]

            if is_finished:
                set_clauses.append('is_finished = TRUE')
            else:
                set_clauses.append('current_page = %s')
                set_clauses.append('note = %s')
                params.append(current_page)
                params.append(note)

            if total_count is not None:
                set_clauses.append('total_count = %s')
                params.append(total_count)

            params.extend([update_version, rights_holder])
            cursor.execute(
                f'UPDATE update_version SET {", ".join(set_clauses)} '
                'WHERE update_version = %s AND rights_holder = %s',
                params,
            )


def get_next_update_version(rights_holder):
    """從 PostgreSQL update_version table 取得此 rightsHolder 下一個 update_version。

    - 沒跑過此 rightsHolder：回傳 1
    - 上一輪已完成 (is_finished=TRUE)：回傳上版本 + 1
    - 上一輪未完成：回傳同版本（供續跑）
    """
    with psycopg2.connect(**db_settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT update_version, is_finished FROM update_version '
                'WHERE rights_holder = %s '
                'ORDER BY update_version DESC LIMIT 1',
                (rights_holder,),
            )
            res = cursor.fetchone()
            if not res:
                return 1
            version, is_finished = res
            return version + 1 if is_finished else version


def init_update_session(rights_holder, records_batch_size=200, matchlog_batch_size=300):
    """初始化 update session：取得 version、續跑 checkpoint、now、processors、dedup_tracker"""
    update_version = get_next_update_version(rights_holder)
    current_page, note = insert_new_update_version(
        rights_holder=rights_holder, update_version=update_version
    )
    dedup_tracker = DedupTracker(rights_holder, update_version)
    # 結束時自動匯出重複紀錄 CSV（含 within_batch 與 cross_batch）
    atexit.register(dedup_tracker.export_duplicates_csv)
    return UpdateSession(
        update_version=update_version,
        current_page=current_page,
        note=note,
        now=datetime.now() + timedelta(hours=8),
        records_processor=OptimizedRecordsProcessor(db, batch_size=records_batch_size),
        matchlog_processor=OptimizedMatchLogProcessor(db, batch_size=matchlog_batch_size),
        dedup_tracker=dedup_tracker,
    )