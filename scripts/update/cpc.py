import requests
import pandas as pd
import time
import json
from app import engine
from scripts.utils.common import *
from scripts.utils.deduplicates import DedupTracker, resolve_existed_records
from scripts.utils.records import OptimizedRecordsProcessor, prepare_df_for_sql, delete_records
from scripts.utils.match import OptimizedMatchLogProcessor, process_match_log, process_taxon_match, zip_match_log
from scripts.utils.geography import process_geo_batch, geo_keys
from scripts.utils.export import export_records_with_taxon
from scripts.utils.update_version import init_update_session, update_update_version
from scripts.utils.dataset import process_dataset, update_dataset_deprecated, fetch_taibif_datasets
from tqdm import tqdm
from scripts.utils.progress import timer
import atexit

records_processor = OptimizedRecordsProcessor(engine, batch_size=200)
matchlog_processor = OptimizedMatchLogProcessor(engine, batch_size=300)

# 比對學名時使用的欄位
sci_cols = ['taxonID','sourceVernacularName', 'sourceScientificName','originalVernacularName','sourceClass','sourceOrder', 'sourceFamily','sourceKingdom']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'cpc'
rights_holder = '中油生態地圖'
info_id = 0

# 更新紀錄
session = init_update_session(rights_holder)
update_version = session.update_version
current_page = session.current_page
note = session.note 
now = session.now
records_processor = session.records_processor
matchlog_processor = session.matchlog_processor

# 更新失敗紀錄
atexit.register(records_processor.export_failed_records, 
                f'failed_records_{group}_{info_id}.csv')
atexit.register(matchlog_processor.export_failed_records, 
                f'failed_match_logs_{group}_{info_id}.csv')


dedup_tracker = DedupTracker(rights_holder, update_version)


if not note:
    d_list_index = 0
    dataset_list = []
else:
    d_list_index = note.get('d_list_index')
    dataset_list = note.get('dataset_list')


# 取得中油發布資料集
dataset, dataset_list = fetch_taibif_datasets(publisher_id='617995df-635d-49cb-8ce0-2ab8bc0cfe7a')
should_stop = False

for d in dataset_list[d_list_index:]:
    c = current_page
    has_more_data = True
    total_count = None
    while has_more_data: # 尚未到資料集的總數
        data = []
        media_rule_list = []
        p = c + 10
        while c < p: # 每次處理10頁 還沒到十頁的時候不中斷
            time.sleep(1)
            offset = 1000 * c
            print(d[0], d[1], '/ page:',c , ' , offset:', offset)
            url = f"https://portal.taibif.tw/api/v3/occurrence?taibifDatasetID={d[0]}&rows=1000&offset={offset}"
            response = requests.get(url)
            if response.status_code == 200:
                result = response.json()
                data += result.get('data')
                if total_count is None:
                    total_count = result.get('count') if result.get('count') else 0
                else:
                    if isinstance(result.get('count'), int):
                        if result.get('count') > total_count:
                            total_count = result.get('count')
                if offset + 1000 >= total_count:
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
            # 如果 'taxonBackbone' == 'TaiCOL' 給予taxonID
            df['taxonID'] = df['scientificNameID'].where(df['taxonBackbone'] == 'TaiCOL')
            df = filter_by_license_and_sensitivity(df)
            if len(df):
                df = df.reset_index(drop=True)
                df = df.replace(to_quote_dict)
                df = df.rename(columns= {'originalOccurrenceID': 'occurrenceID',
                                        'taibifOccurrenceID': 'sourceOccurrenceID',
                                        'taibifScientificName': 'sourceScientificName',
                                        'originalScientificName': 'originalVernacularName',
                                        'taxonRank': 'sourceTaxonRank',
                                        'vernacularName': 'sourceVernacularName',
                                        'family': 'sourceFamily',
                                        'class': 'sourceClass',
                                        'order': 'sourceOrder',
                                        'kingdom': 'sourceKingdom',
                                        'decimalLatitude': 'verbatimLatitude',
                                        'decimalLongitude': 'verbatimLongitude',
                                        'taibifModifiedDate': 'sourceModified',
                                        'taibifDatasetID': 'sourceDatasetID'})
                df = df.drop(columns=['taxonGroup','taxonBackbone','phylum','genus','geodeticDatum', 'countryCode', 
                                      'country', 'county', 'habitatReserve', 'wildlifeReserve', 'occurrenceStatus', 'selfProduced',
                                      'datasetShortName','establishmentMeans', 'issue'])
                df = process_taxon_match(df, sci_cols)
                df = apply_common_fields(df, group, rights_holder, now)
                df = apply_record_type(df, mode='auto')
                df, media_rule_list = apply_media_rule(df, [])
                df[geo_keys] = process_geo_batch(df, skip_blur=True) # 無敏感資料
                df = df.replace(to_quote_dict)
                df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
                df['datasetURL'] = 'https://www.gbif.org/dataset/' + df['gbifDatasetID'].fillna('')
                df = process_dataset(df, group, rights_holder, update_version, now,
                      extra_cols=None,
                      dataset=dataset,
                      df_cols=['datasetName', 'gbifDatasetID', 'sourceDatasetID', 'datasetURL'],
                      dataset_cols=['taibifDatasetID', 'datasetPublisher', 'datasetLicense'],
                      left_on='sourceDatasetID',
                      right_on='taibifDatasetID')
                df, existed_records = resolve_existed_records(df, rights_holder, dedup_tracker)
                df = update_gbif_references(df, existed_records)
                df = df.replace(to_none_dict)
                process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=f"{d_list_index}_{c}")
                df = prepare_df_for_sql(df, update_version)
                records_processor.smart_upsert_records(df, existed_records=existed_records)
                export_records_with_taxon(df, f'/solr/csvs/export/{group}_{info_id}_{d_list_index}_{c}.csv')
                update_media_rules(media_rules=media_rule_list,rights_holder=rights_holder, now=now)
        # 成功之後 更新update_update_version
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))
    d_list_index += 1
    current_page = 0 # 換成新的url時要重新開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=0, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))

if not has_more_data:
    delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
    zip_match_log(group=group,info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)

print('done!')