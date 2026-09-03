import requests
import pandas as pd
import time
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
sci_cols = ['sourceScientificName','sourceVernacularName','sourceFamily','sourceKingdom']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'hast'
rights_holder = '中央研究院生物多樣性中心植物標本資料庫'
info_id = 0

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

c = current_page
has_more_data = True
pbar = None

while has_more_data:
    data = []
    p = c + 10
    while c < p and has_more_data:
        offset = 300 * c
        time.sleep(1)
        url = f"https://hast.biodiv.tw/api/v1/occurrence?offset={offset}"
        response = requests.get(url, verify=True)
        if response.status_code == 200:
            result = response.json()
            total_page = result['meta']['pagination']['num_pages']
            data += result.get('data')
            if pbar is None:
                pbar = tqdm(total=total_page, unit='頁', desc=group)
                if c > 1:
                    pbar.update(c - 1)      # resume 快進
            pbar.update(1)
            if c >= total_page:
                has_more_data = False
                break
            c+=1
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
                                    'scientificName': 'sourceScientificName', 
                                    'isPreferredName': 'sourceVernacularName', 
                                    'collectionID': 'catalogNumber', 
                                    'taxonRank': 'sourceTaxonRank',
                                    'family': 'sourceFamily'})
            df['sourceKingdom'] = 'Plantae'
            df = process_taxon_match(df, sci_cols)
            df = apply_common_fields(df, group, rights_holder, now)
            # TODO 這邊可能會改成有提供，待確認
            df = apply_record_type(df, mode='col')  # basisOfRecord 無資料
            df, media_rule_list = apply_media_rule(df, [])
            df[geo_keys] = process_geo_batch(df, skip_blur=True) # 敏感層級 無資料
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
            process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=c)
            export_records_with_taxon(df_for_sql,f'/solr/csvs/export/{group}_{info_id}_{c}.csv')
            update_media_rules(media_rules=media_rule_list,rights_holder=rights_holder, now=now)
            timer.batch_summary(label=f"批次 c={c}")
    # 成功之後 更新update_update_version
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=None, total_count=records_processor.success_count)

if pbar:
    pbar.close()

if not has_more_data:
    failed_tbia_ids = {r['tbiaID'] for r in records_processor.failed_records if r.get('tbiaID')}
    delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version),exclude_ids=failed_tbia_ids)
    zip_match_log(group=group,info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder, total_count=records_processor.success_count)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)
    print('done!')