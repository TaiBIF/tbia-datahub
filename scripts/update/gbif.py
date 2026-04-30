# for dataset in dataset_list:
# │
# ├─ while not is_reaching_end:   ← 一次處理 10 頁的單位
# │   │
# │   ├─ while c < p:              ← 一頁一頁抓，最多抓 10 頁
# │   │   ├─ 抓 API（offset = 1000 * c）
# │   │   ├─ 加入 data[]
# │   │   ├─ 判斷是否為最後一頁（offset + 1000 >= total_count）
# │   │   └─ 若不是最後一頁，c += 1
# │   │
# │   └─ 把這 10 頁資料做後處理（例如轉成 df）
# │
# └─ 換下一個 dataset（d_list_index += 1）

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

records_processor = OptimizedRecordsProcessor(engine, batch_size=200)
matchlog_processor = OptimizedMatchLogProcessor(engine, batch_size=300)

# 比對學名時使用的欄位
sci_cols = ['taxonID','sourceVernacularName', 'sourceScientificName', 'originalVernacularName', 'sourceClass','sourceOrder', 'sourceFamily', 'sourceKingdom']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'gbif'
rights_holder = 'GBIF'
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


if not note:
    d_list_index = 0
    dataset_list = []
else:
    d_list_index = note.get('d_list_index')
    dataset_list = note.get('dataset_list')


# 排除夥伴單位
partners = ['6ddd1cf5-0655-44ac-a572-cb581a054992', # 林保署
            '898ba450-1627-11df-bd84-b8a03c50a862', # 林試所
            '7f2ff82e-193e-48eb-8fb5-bad64c84782a', # 國家公園
            'f40c7fe5-e64a-450c-b229-21d674ef3c28', # 國家公園
            'c57cd401-ff9e-43bd-9403-089b88a97dea', # 台博館
            'b6b89e2d-e881-41f3-bc57-213815cb9742', # 水利署
            '3eff04f7-c90b-4aae-ad2e-9bbdb225ba69', # 科博館
            '617995df-635d-49cb-8ce0-2ab8bc0cfe7a', # 中油
            ]

# 排除重複資料集
# 單位間
# GBIF 需要排除的生多所資料
duplicated_dataset_list = [
    '4fa7b334-ce0d-4e88-aaae-2e0c138d049e',
    'af97275b-4603-4b87-9054-c83c71c45143',
    '471511f5-beca-425f-9a8a-e802b3960906',
    'bc76c690-60a3-11de-a447-b8a03c50a862',
    'a0998d3b-4a7f-4add-8044-299092d9c63f',
    'a9d518d1-f0f3-477b-a7a3-aa9f61eb1e54',
    'ea9608d2-7101-4d46-a7d0-9add260cd28c',
    'e34125ac-b4fd-4ad4-9647-3423cdd9b8a2',
    'b6fccb11-dc9a-4cf6-9994-b46fbac5759f',
    '19c3400b-b7bb-425f-b8c5-f222648b86b2',
    '2de58bfe-1bf1-4318-97a3-d97efc269a4f',
    '9e6bf53c-8dba-470a-9142-3607dfe21c41',
    'd4919a44-090f-4cc6-8643-4c5f7906117f',
    '6bd0551c-f4e9-4e85-9cec-6cefae343234'
]


dataset, dataset_list = fetch_taibif_datasets(
    source='GBIF',
    exclude_publishers=partners,
    exclude_gbif_datasets=duplicated_dataset_list,
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
            print('data', len(data))
            df = pd.DataFrame(data)
            df = df.replace(to_quote_dict)
            # 如果 'taxonBackbone' == 'TaiCOL' 給予taxonID
            df['taxonID'] = df['scientificNameID'].where(df['taxonBackbone'] == 'TaiCOL')
            # 如果學名相關的欄位都是空值才排除
            df = filter_by_taxon_fields(df, required_cols=['taibifScientificName','vernacularName','originalScientificName','class','order','family','taxonID'])
            df = filter_by_license_and_sensitivity(df)
            if len(df):
                df = df.reset_index(drop=True)
                df = df.replace(to_quote_dict)
                df = df.rename(columns= {
                                        'originalOccurrenceID': 'sourceOccurrenceID',
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
        print('saved', c)
    d_list_index += 1
    current_page = 0 # 換成新的url時要重新開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=0, note=json.dumps({'d_list_index': d_list_index, 'dataset_list': dataset_list}))

if not has_more_data:
    delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
    zip_match_log(group=group,info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)
    records_processor.export_failed_records(f'failed_records_{group}_{info_id}.csv')
    matchlog_processor.export_failed_records(f'failed_match_logs_{group}_{info_id}.csv')


print('done!')