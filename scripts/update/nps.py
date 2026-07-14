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
sci_cols = ['sourceScientificName','sourceVernacularName']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'nps'
rights_holder = '臺灣國家公園生物多樣性資料庫'
info_id = 0

# 排除重複資料集
duplicated_dataset_list = ['臺灣林業試驗所植物標本館','國立臺灣博物館典藏數位化計畫']

locality_map = {
    'YMS': '陽明山國家公園',
    'TRK': '太魯閣國家公園',
    'SP': '雪霸國家公園',
    'ES': '玉山國家公園',
    'KT': '墾丁國家公園',
    'KM': '金門國家公園',
    'DS': '東沙環礁國家公園',
    'TJ': '台江國家公園',
    'SNNP': '壽山國家自然公園',
    'SSNP': '壽山國家自然公園',
    'SPM': '澎湖南方四島國家公園',
    'TCMP': '台中都會公園',
    'KCMP': '高雄都會公園',
}


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

c = current_page if current_page != 0 else 1

has_more_data = True
should_stop = False

while has_more_data:
    data = []
    p = c + 10
    while c < p: # 每次處理10頁 還沒到十頁的時候不中斷
        url = f"https://npgis.nps.gov.tw/TBiAOpenApi/api/Data/Get?Token={os.getenv('CPAMI_KEY')}&Page={c}"
        response = requests.get(url)
        if response.status_code == 200:
            result = response.json()
            total_page = result['Meta']['TotalPages']
            data += result.get('Data')
            print(c, total_page)
            if c >= total_page:
                has_more_data = False
                break
            c+=1
        else:
            print(f"Error: HTTP {response.status_code}")
            should_stop = True
            break  # 跳出內層 while
    if should_stop:
        break # 跳出外層 while
    if len(data):
        df = pd.DataFrame(data)
        df = df.replace(to_quote_dict)
        df = filter_by_license_and_sensitivity(df)
        if len(df):
            df = df.reset_index(drop=True)
            df = df.replace(to_quote_dict)
            df = df.rename(columns={'basicOfRecord': 'basisOfRecord', 
                                    'created': 'sourceCreated', 
                                    'modified': 'sourceModified', 
                                    'scientificName': 'sourceScientificName', 
                                    'isPreferredName': 'sourceVernacularName', 
                                    'taxonRank': 'sourceTaxonRank'})
            df = process_taxon_match(df, sci_cols)
            df['locality'] = df['locality'].apply(lambda x: locality_map[x] if x in locality_map.keys() else x)
            df = apply_common_fields(df, group, rights_holder, now)
            df = apply_record_type(df, mode='auto') 
            df, media_rule_list = apply_media_rule(df, [])
            df[geo_keys] = process_geo_batch(df, is_full_hidden='auto')
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
    # 成功之後 更新update_update_version
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=None, total_count=records_processor.success_count)

if not has_more_data:
    failed_tbia_ids = {r['tbiaID'] for r in records_processor.failed_records if r.get('tbiaID')}
    delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version),exclude_ids=failed_tbia_ids)
    # delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
    zip_match_log(group=group,info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder, total_count=records_processor.success_count)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)


print('done!')