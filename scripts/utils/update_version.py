# 紀錄update相關的參數

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import requests
from scripts.utils.sql import OptimizedRecordsProcessor, OptimizedMatchLogProcessor
from app import db, db_settings
import psycopg2


@dataclass
class UpdateSession:
    update_version: int
    current_page: int
    note: Optional[str]
    now: datetime
    records_processor: 'OptimizedRecordsProcessor'
    matchlog_processor: 'OptimizedMatchLogProcessor'


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


def update_update_version(update_version, rights_holder, current_page=0, note=None, is_finished=False):
    now = datetime.now() + timedelta(hours=8)
    with psycopg2.connect(**db_settings) as conn:
        with conn.cursor() as cursor:
            if is_finished:
                cursor.execute(
                    'UPDATE update_version SET is_finished = TRUE, modified = %s '
                    'WHERE update_version = %s AND rights_holder = %s',
                    (now, update_version, rights_holder),
                )
            else:
                cursor.execute(
                    'UPDATE update_version SET current_page = %s, note = %s, modified = %s '
                    'WHERE update_version = %s AND rights_holder = %s',
                    (current_page, note, now, update_version, rights_holder),
                )


def get_next_update_version(rights_holder):
    """從 Solr 取得此 rightsHolder 下一個 update_version"""
    url = (
        'http://solr:8983/solr/tbia_records/select'
        '?fl=update_version'
        f'&fq=rightsHolder:"{rights_holder}"'
        '&q.op=OR&q=*%3A*&rows=1&sort=update_version%20desc'
    )
    response = requests.get(url)
    response.raise_for_status()
    docs = response.json()['response']['docs']
    return docs[0]['update_version'] + 1 if docs else 1


def init_update_session(rights_holder, records_batch_size=200, matchlog_batch_size=300):
    """初始化 update session：取得 version、續跑 checkpoint、now、processors"""
    update_version = get_next_update_version(rights_holder)
    current_page, note = insert_new_update_version(
        rights_holder=rights_holder, update_version=update_version
    )
    return UpdateSession(
        update_version=update_version,
        current_page=current_page,
        note=note,
        now=datetime.now() + timedelta(hours=8),
        records_processor=OptimizedRecordsProcessor(db, batch_size=records_batch_size),
        matchlog_processor=OptimizedMatchLogProcessor(db, batch_size=matchlog_batch_size),
    )