import requests
import pandas as pd
import time
import json
from app import engine
from scripts.utils.common import *
from scripts.utils.deduplicates import resolve_existed_records
from scripts.utils.records import prepare_df_for_sql, delete_records
from scripts.utils.match import process_match_log, process_taxon_match, zip_match_log
from scripts.utils.geography import process_geo_batch, geo_keys
from scripts.utils.export import export_records_with_taxon
from scripts.utils.update_version import init_update_session, update_update_version
from scripts.utils.dataset import process_dataset, update_dataset_deprecated
from tqdm import tqdm
from scripts.utils.progress import timer
import atexit

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName', 'sourceVernacularName']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'ascdc'
rights_holder = '臺灣魚類資料庫'
info_id = 0

base_url_list = [
    'https://datahub.openmuseum.tw/api/v2/fishdb_occurrence/list',
    'https://datahub.openmuseum.tw/api/v2/fa_occurrence/list',
]

# 更新紀錄
session = init_update_session(rights_holder)
update_version = session.update_version
current_page = session.current_page
note = session.note
now = session.now
records_processor = session.records_processor
matchlog_processor = session.matchlog_processor
dedup_tracker = session.dedup_tracker

# 更新失敗紀錄
atexit.register(records_processor.export_failed_records,
                f'failed_records_{group}_{info_id}.csv')
atexit.register(matchlog_processor.export_failed_records,
                f'failed_match_logs_{group}_{info_id}.csv')

if not note:
    url_index = 0
else:
    url_index = note.get('url_index', 0)

for u_idx in range(url_index, len(base_url_list)):
    base_url = base_url_list[u_idx]
    c = current_page if current_page != 0 else 1
    has_more_data = True
    total_count = None
    pbar = None

    while has_more_data:
        data = []
        p = c + 10
        while c < p:  # 每次處理10頁 還沒到十頁的時候不中斷
            time.sleep(1)
            url = f"{base_url}?token={os.getenv('ASCDC_KEY')}&page={c}&per_page=100"
            response = requests.get(url, verify=False, headers={'user-agent': "TBIA"})
            if response.status_code == 200:
                result = response.json()
                batch = result.get('data')
                data += batch
                # 追蹤總數（分頁間 total 可能變動，取最大值）
                if total_count is None:
                    total_count = result['meta']['total'] if result['meta']['total'] else 0
                elif isinstance(result['meta']['total'], int) and result['meta']['total'] > total_count:
                    total_count = result['meta']['total']
                # 進度條：第一次拿到總數才建立；總數變大就同步刷新
                if pbar is None:
                    pbar = tqdm(total=total_count, unit='筆', desc=base_url.split('/')[-2])
                    if c > 1:
                        pbar.update((c - 1) * 100)   # resume 快進
                elif pbar.total != total_count:
                    pbar.total = total_count
                    pbar.refresh()
                pbar.update(len(batch))
                if c * 100 >= total_count:  # 當前的頁數已經超過總數
                    has_more_data = False
                    break
                c += 1
            else:
                raise Exception(f"API failed: {response.status_code} - {url}")
        if len(data):
            df = pd.DataFrame(data)
            df = df.replace(to_quote_dict)
            df = filter_by_license_and_sensitivity(df)
            if len(df):
                df = df.reset_index(drop=True)
                df = df.replace(to_quote_dict)
                df = df.rename(columns={'created': 'sourceCreated',
                                        'modified': 'sourceModified',
                                        'scientificName': 'sourceScientificName'})
                df = df.drop(columns=['subject', 'planningAgency', 'executiveAgency', 'provider'], errors='ignore')
                df['sourceVernacularName'] = ''  # 補上
                df = process_taxon_match(df, sci_cols)
                df = apply_common_fields(df, group, rights_holder, now)
                df = apply_record_type(df, mode='occ')  # basisOfRecord 無資料
                df, media_rule_list = apply_media_rule(df, [])
                df[geo_keys] = process_geo_batch(df, skip_blur=True)
                df = df.replace(to_quote_dict)
                df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
                df = process_dataset(df, group, rights_holder, update_version, now)
                df, existed_records = resolve_existed_records(df, rights_holder, dedup_tracker)
                df = df.replace(to_none_dict)
                df_for_sql = prepare_df_for_sql(df, update_version)
                failed_ids = records_processor.smart_upsert_records(
                    df_for_sql, existed_records=existed_records, dedup_tracker=dedup_tracker
                )
                if failed_ids:
                    df = df[~df['id'].isin(failed_ids)].reset_index(drop=True)
                    df_for_sql = df_for_sql[~df_for_sql['tbiaID'].isin(failed_ids)].reset_index(drop=True)
                process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=f"{u_idx}_{c}")
                export_records_with_taxon(df_for_sql, f'/solr/csvs/export/{group}_{info_id}_{u_idx}_{c}.csv')
                update_media_rules(media_rules=media_rule_list, rights_holder=rights_holder, now=now)
                timer.batch_summary(label=f"{base_url.split('/')[-2]} c={c}")
        # 成功之後 更新update_update_version
        update_update_version(update_version=update_version, rights_holder=rights_holder,
                              current_page=c, note=json.dumps({'url_index': u_idx}),
                              total_count=records_processor.success_count)

    if pbar:
        pbar.close()

    current_page = 0  # 換成新的url時要重新開始
    update_update_version(update_version=update_version, rights_holder=rights_holder,
                          current_page=0, note=json.dumps({'url_index': u_idx + 1}),
                          total_count=records_processor.success_count)

# 全部 url 跑完（含續跑到最後、for 空轉）才收尾；base_url_list 寫死非空，故無條件執行
failed_tbia_ids = {r['tbiaID'] for r in records_processor.failed_records if r.get('tbiaID')}
delete_records(rights_holder=rights_holder, group=group, update_version=int(update_version), exclude_ids=failed_tbia_ids)
zip_match_log(group=group, info_id=info_id)
update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder, total_count=records_processor.success_count)
update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)
print('done!')