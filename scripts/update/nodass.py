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
from tqdm import tqdm
from scripts.utils.progress import timer
import atexit

records_processor = OptimizedRecordsProcessor(engine, batch_size=200)
matchlog_processor = OptimizedMatchLogProcessor(engine, batch_size=300)

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName','sourceVernacularName','sourceClass','sourceOrder','sourceFamily','sourceKingdom']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'namr'
rights_holder = '國家海洋資料庫及共享平台'
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


url = 'https://nodass.namr.gov.tw/noapi/namr/v1/list/organism/project/NAMR'
resp = requests.get(url)

data = []
# 目前總數不多 可以一次取得所有資料就好

for r in resp.json():
    now_url = r.get('AccessURL')
    now_resp = requests.get(now_url)
    data += now_resp.json()


if len(data):
    df = pd.DataFrame(data)
    df = df.replace(to_quote_dict)
    # 補上license
    df['license'] = 'OGDL'
    df = filter_by_license_and_sensitivity(df)
    if len(df):
        df = df.reset_index(drop=True)
        df = df.replace(to_quote_dict)
        df = df.rename(columns={'Id': 'occurrenceID',
                                'ProjectID': 'sourceDatasetID',
                                'ProjectName': 'datasetName',
                                'Organizer': 'rightsHolder',
                                'ScientificName': 'sourceScientificName',
                                'ChineseName': 'sourceVernacularName',
                                'ClassName': 'sourceClass',
                                'OrderName': 'sourceOrder',
                                'FamilyName': 'sourceFamily',
                                'KingdomName': 'sourceKingdom',
                                'RecordedByChinese': 'recordedBy',
                                'IndividualCount': 'individualCount',
                                'FillingDate': 'eventDate',
                                'AccessURL': 'associatedMedia'})
        df = df.drop(columns=['ScientificNameAuthorship','ChineseCommonName','EnglishCommonName','OriginalRecord','OrganismType',
                                'KingdomChineseName','PhylumName','PhylumChineseName','ClassChineseName','OrderChineseName','FamilyChineseName',
                                'GenusName','GenusChineseName','IdentifiedBy','IdentifiedByChinese','RecordedBy','SamplingProtocol','Duration','Area','AreaUnit',
                                'OrganismDensity','SampleSizeValue','SampleSizeUnit','CoralReefCoverage','CoverageUnit','MeasurementRemarks','OrganismRemarks','RecordType'], errors='ignore')
        df = process_taxon_match(df, sci_cols)
        df = apply_common_fields(df, group, rights_holder, now)
        df = apply_record_type(df, mode='occ')  # basisOfRecord 無資料
        # 從 geom 取出經緯度
        df['verbatimLongitude'] = df['geom'].apply(lambda x: x.get('coordinates')[0] if x else None)
        df['verbatimLatitude'] = df['geom'].apply(lambda x: x.get('coordinates')[1] if x else None)
        # 補 mediaLicense (associatedMedia 有值才補 OGDL)
        df['mediaLicense'] = df['associatedMedia'].map(lambda x: 'OGDL' if x else None)
        df, media_rule_list = apply_media_rule(df, [])
        df[geo_keys] = process_geo_batch(df, skip_blur=True) # 無敏感資料
        df = df.replace(to_quote_dict)
        df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
        df = process_dataset(df, group, rights_holder, update_version, now,
                extra_cols=['sourceDatasetID'],
                dataset=None, df_cols=None, dataset_cols=None,
                      left_on=None, right_on=None)
        df, existed_records = resolve_existed_records(df, rights_holder, dedup_tracker)
        df = df.replace(to_none_dict)
        process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=None)
        df = prepare_df_for_sql(df, update_version)
        records_processor.smart_upsert_records(df, existed_records=existed_records)
        export_records_with_taxon(df, f'/solr/csvs/export/{group}_{info_id}.csv')
        update_media_rules(media_rules=media_rule_list,rights_holder=rights_holder, now=now)

delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
zip_match_log(group=group,info_id=info_id)
update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)
update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)


print('done!')