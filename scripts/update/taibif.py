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
sci_cols = ['taxonID','sourceVernacularName', 'sourceScientificName','originalVernacularName','sourceClass','sourceOrder', 'sourceFamily', 'sourceKingdom']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'brcas'
rights_holder = '臺灣生物多樣性資訊機構 TaiBIF'
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


partners = ['6ddd1cf5-0655-44ac-a572-cb581a054992', # 林保署
            '7c07cec1-2925-443c-81f1-333e4187bdea', # 生多所
            '898ba450-1627-11df-bd84-b8a03c50a862', # 林試所
            '7f2ff82e-193e-48eb-8fb5-bad64c84782a', # 國家公園
            'f40c7fe5-e64a-450c-b229-21d674ef3c28', # 國家公園
            'c57cd401-ff9e-43bd-9403-089b88a97dea', # 台博館
            'b6b89e2d-e881-41f3-bc57-213815cb9742', # 水利署
            '3eff04f7-c90b-4aae-ad2e-9bbdb225ba69', # 科博館
            '617995df-635d-49cb-8ce0-2ab8bc0cfe7a', # 中油
            'fec00ed1-a0f3-440b-b10c-d6dca979ade1', # 中研院植標館 先暫時排除
            ]

# 202507 目前生多中心 & 生多博物館的都歸在TaiBIF底下 不須獨立出來

# 排除重複資料集
# 單位間
duplicated_dataset_list = [
    '36c38933-a03b-4f8b-9ba3-6987e5528179',
    '489b921b-88fe-40ca-9efc-dbb3270bfa9e',
    'ec70c946-482c-4e10-ab56-9e190c9d40f9',
    'fddbabb3-7386-4a1c-a086-f12bbabe9eb6',
    '44a761b5-5adf-4b67-adad-c5ae04637fb9',
    '06b55da4-bfb9-453d-be18-a1d1ae68ed5d',
    '836a5bd1-d440-4ebd-bb1e-0d83f91bd21a',
    'af48a08e-f523-443d-9d4d-505a01be11a4',
    '07b06590-6ecc-489e-a565-73c1f2081a02',
    '73f63477-81be-4661-8d71-003597a701c0',
    'e7b6eb08-1380-40c7-9a2e-60d2ac9b00c2',
    'c6552cda-cdb3-4711-84c1-347c6fe8ba86',
]
# 單位內
duplicated_dataset_list += ['6e54a298-6358-4994-ae50-df9a8dd4efc6']


dataset, dataset_list = fetch_taibif_datasets(
    source='not_GBIF',
    exclude_publishers=partners,
    exclude_datasets=duplicated_dataset_list,
    only_tw_publishers=True,
)
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
                df = df.rename(columns= {'originalOccurrenceID': 'sourceOccurrenceID',
                                        'taibifOccurrenceID': 'occurrenceID', # 使用TaiBIF給的id, 避免空值
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
                df = df.drop(columns=['taxonGroup','taxonBackbone','phylum','genus','geodeticDatum',
                                    'countryCode', 'country', 'county',
                                    'habitatReserve', 'wildlifeReserve', 'occurrenceStatus', 'selfProduced',
                                    'datasetShortName','establishmentMeans', 'issue'])
                df = process_taxon_match(df, sci_cols)
                df = apply_common_fields(df, group, rights_holder, now)
                df = apply_record_type(df, mode='auto')
                df, media_rule_list = apply_media_rule(df, [])
                df[geo_keys] = process_geo_batch(df, skip_blur=True) # 無敏感資料
                df = df.replace(to_quote_dict)
                df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
                df['datasetURL'] = 'https://portal.taibif.tw/dataset/' + df['sourceDatasetID'].fillna('')
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
        # 成功之後 更新update_update_version (有可能某次的data完全沒有符合的資料 那也要紀錄已經跑過了c)
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))
        print('saved', c)
    d_list_index += 1
    current_page = 0 # 換成新的url時要重新開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=0, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))

if not has_more_data:
    delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
    zip_match_log(group=group,info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)


print('done!')