import requests
import pandas as pd
import time
from app import engine
import json
from scripts.utils.common import *
from scripts.utils.deduplicates import DedupTracker, resolve_existed_records
from scripts.utils.records import OptimizedRecordsProcessor, prepare_df_for_sql, delete_records
from scripts.utils.match import OptimizedMatchLogProcessor, process_match_log, process_taxon_match, zip_match_log
from scripts.utils.geography import process_geo_batch, geo_keys
from scripts.utils.export import export_records_with_taxon
from scripts.utils.dataset import fetch_tbn_datasets
from scripts.utils.update_version import init_update_session, update_update_version
from scripts.utils.dataset import process_dataset, update_dataset_deprecated

records_processor = OptimizedRecordsProcessor(engine, batch_size=200)
matchlog_processor = OptimizedMatchLogProcessor(engine, batch_size=300)

# 比對學名時使用的欄位
sci_cols = ['taxonID','sourceScientificName','sourceVernacularName','originalVernacularName','sourceTaxonID','sourceFamily']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'tbri'
rights_holder = '台灣生物多樣性網絡 TBN'
info_id = 0

# 更新紀錄
session = init_update_session(rights_holder)
update_version = session.update_version
current_page = session.current_page
note = session.note 
now = session.now
records_processor = session.records_processor
matchlog_processor = session.matchlog_processor

# 取得dataset info
url_list, dataset = fetch_tbn_datasets()

dedup_tracker = DedupTracker(rights_holder, update_version)

request_url, url_index = (note.get('request_url'), note.get('url_index')) if note else (None, 0)

for url in url_list[url_index:]:
    if not request_url:
        request_url = url
    c = current_page
    data = []
    while request_url:
        time.sleep(0.5)
        if request_url.find('limit=1000') < 0:
            request_url += '&limit=1000'
        if request_url.find(f"apikey={os.getenv('TBN_KEY')}") < 0:
            request_url += f"&apikey={os.getenv('TBN_KEY')}"
        response = requests.get(request_url)
        if response.status_code == 200:
            result = response.json()
            total_count = result['meta']['total']
            print(c, ',', (c+1)*1000, '/', total_count, ',', request_url)
            request_url = result['links']['next']
            data += result["data"]
            c += 1
        else:
            raise Exception(f"API failed: {response.status_code} - {request_url}")
        if c % 10 == 0 or not request_url:
            df = pd.DataFrame(data)
            df = df.replace(to_quote_dict)
            df['originalVernacularName'] = df['originalVernacularName'].replace({'原始資料無物種資訊': ''})
            # 如果學名相關的欄位都是空值才排除
            df = filter_by_taxon_fields(df, required_cols=['originalVernacularName','simplifiedScientificName','vernacularName','familyScientificName','taiCOLTaxonID'])
            df = filter_by_license_and_sensitivity(df)
            if len(df):
                df = df.reset_index(drop=True)
                df = df.replace(to_quote_dict)
                df = df.rename(columns={
                    'created': 'sourceCreated',
                    'modified': 'sourceModified',
                    'simplifiedScientificName': 'sourceScientificName',
                    'decimalLatitude': 'verbatimLatitude', 
                    'decimalLongitude': 'verbatimLongitude',
                    'geodeticDatum': 'verbatimSRS',
                    'taiCOLTaxonID': 'taxonID',
                    'taxonUUID': 'sourceTaxonID',
                    'taxonRank': 'sourceTaxonRank',
                    'vernacularName': 'sourceVernacularName',
                    'familyScientificName': 'sourceFamily',
                    'datasetUUID': 'sourceDatasetID'
                })
                df['locality'] = df.apply(lambda x: x.county + x.municipality if not x.sensitiveCategory == '座標不開放' else '', axis = 1) #
                # 若沒有individualCount 則用organismQuantity 
                df['organismQuantity'] = df.apply(lambda x: x.individualCount if x.individualCount else x.organismQuantity, axis = 1)
                df = df.drop(columns=['externalID','minimumElevationInMeters','gridID','adminareaCode',
                                    'county','municipality','hour','minute','protectedStatusTW',
                                        'categoryIUCN', 'categoryRedlistTW', 'endemism', 'nativeness',
                                        'taxonGroup','scientificName','taiCOLNameCode','familyVernacularName', 'datasetAuthor', 
                                        'resourceCitationIdentifier','establishmentMeans','individualCount','partner',
                                        'identificationVerificationStatus', 'identifiedBy', 'dataSensitiveCategory',
                                        'eventID', 'samplingProtocol','source','selfProduced',
                                        'collectionID','verbatimEventDate','eventTime', 'eventPlaceAdminarea',
                                        'countyCode','tfNameCode', 'scientificNameID'],errors='ignore')
                df['taxonID'] = df['taxonID'].apply(lambda x: x if len(str(x)) == 8 else '')
                # NOTE 應該在這邊就先用sci_index和原本的df merge 才不會後面有複合種的問題
                df = process_taxon_match(df, sci_cols)
                df = apply_common_fields(df, group, rights_holder, now)
                df['references'] = df.apply(lambda x: f"https://www.tbn.org.tw/occurrence/{x.occurrenceID}" if x.occurrenceID else None, axis=1)
                df = apply_record_type(df, mode='auto')
                df, media_rule_list = apply_media_rule(df, [])
                df[geo_keys] = process_geo_batch(df, is_full_hidden='auto')
                df = df.replace(to_quote_dict)
                df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
                df = process_dataset(df, group, rights_holder, update_version, now,
                      extra_cols=None,
                      dataset=dataset,
                      df_cols=['datasetName', 'sourceDatasetID', 'datasetURL', 'datasetPublisher'],
                      dataset_cols=['datasetUUID', 'datasetLicense'],
                      left_on='sourceDatasetID',
                      right_on='datasetUUID')
                df, existed_records = resolve_existed_records(df, rights_holder, dedup_tracker)
                df = df.replace(to_none_dict)
                process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=f"{url_index}_{c}")
                df = prepare_df_for_sql(df, update_version)
                records_processor.smart_upsert_records(df, existed_records=existed_records)
                export_records_with_taxon(df, f'/solr/csvs/export/{group}_{info_id}_{url_index}_{c}.csv')
                update_media_rules(media_rules=media_rule_list,rights_holder=rights_holder, now=now)
            # 成功之後 更新update_update_version 也有可能這批page 沒有資料 一樣從下一個c開始
            data = []
            update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=json.dumps({'url_index': url_index, 'request_url': request_url}))
    url_index += 1
    current_page = 0 # 換成新的url時要重新開始
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=0, note=json.dumps({'url_index': url_index, 'request_url': None}))


delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
zip_match_log(group=group,info_id=info_id)
update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)
update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)
records_processor.export_failed_records(f'failed_records_{group}_{info_id}.csv')
matchlog_processor.export_failed_records(f'failed_match_logs_{group}_{info_id}.csv')


print('done!')