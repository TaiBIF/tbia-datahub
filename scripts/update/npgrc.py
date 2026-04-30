import requests
import pandas as pd
import time
from app import engine
from scripts.utils.common import *
from scripts.utils.deduplicates import DedupTracker, resolve_existed_records
from scripts.utils.records import OptimizedRecordsProcessor, prepare_df_for_sql, delete_records
from scripts.utils.match import OptimizedMatchLogProcessor, process_match_log, process_taxon_match, zip_match_log
from scripts.utils.geography import process_geo_batch, geo_keys
from scripts.utils.export import export_records_with_taxon
from scripts.utils.update_version import init_update_session, update_update_version
from scripts.utils.dataset import process_dataset, update_dataset_deprecated

records_processor = OptimizedRecordsProcessor(engine, batch_size=200)
matchlog_processor = OptimizedMatchLogProcessor(engine, batch_size=300)

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName','sourceFamily']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'npgrc'
rights_holder = '作物種原資訊系統'
info_id = 0

# 更新紀錄
session = init_update_session(rights_holder)
update_version = session.update_version
current_page = session.current_page
note = session.note 
now = session.now
records_processor = session.records_processor
matchlog_processor = session.matchlog_processor

dedup_tracker = DedupTracker(rights_holder, update_version)

c = current_page
has_more_data = True


while has_more_data:
    data = []
    p = c + 10
    while c < p and has_more_data:
        offset = c*1000
        print('offset:',offset)
        url = f"https://www.npgrc.tari.gov.tw/NPGRC/DatabaseSearch/get_collection_list?s={os.getenv('NPGRC_KEY')}&offset={offset}&limit=1000"
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            result = response.json()
            data += result.get('data')
            if len(result.get('data')) < 1000:
                has_more_data = False
        c += 1
    if len(data):
        df = pd.DataFrame(data)
        df = df.replace(to_quote_dict)
        # 如果學名相關的欄位都是空值才排除
        df = filter_by_taxon_fields(df, required_cols=['scientificName','isPreferredName','family'])
        df = filter_by_license_and_sensitivity(df)
        if len(df):
            df = df.reset_index(drop=True)
            df = df.replace(to_quote_dict)
            df = df.rename(columns={'created': 'sourceCreated', 
                                    'modified': 'sourceModified', 
                                    'scientificName': 'sourceScientificName',
                                    'isPreferredName': 'sourceVernacularName', 
                                    'family': 'sourceFamily'})
            df['sourceFamily'] = df['sourceFamily'].str.capitalize()
            df = process_taxon_match(df, sci_cols)
            df = apply_common_fields(df, group, rights_holder, now)
            df = apply_record_type(df, mode='col')
            df, media_rule_list = apply_media_rule(df, [])
            # 地理資訊 - 目前無地理資訊
            df = df.replace(to_quote_dict)
            df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
            # 資料集
            df['datasetName'] = '作物種原資訊系統' # 幫忙補
            df = process_dataset(df, group, rights_holder, update_version, now)
            df, existed_records = resolve_existed_records(df, rights_holder, dedup_tracker)
            df = df.replace(to_none_dict)
            process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=c)
            df = prepare_df_for_sql(df, update_version)
            records_processor.smart_upsert_records(df, existed_records=existed_records)
            export_records_with_taxon(df, f'/solr/csvs/export/{group}_{info_id}_{c}.csv')
            update_media_rules(media_rules=media_rule_list,rights_holder=rights_holder, now=now)
    # 成功之後 更新update_update_version 也有可能這批page 沒有資料 一樣從下一個c開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=p, note=None)

if not has_more_data:
    delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
    zip_match_log(group=group,info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)
    records_processor.export_failed_records(f'failed_records_{group}_{info_id}.csv')
    matchlog_processor.export_failed_records(f'failed_match_logs_{group}_{info_id}.csv')


print('done!')