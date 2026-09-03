import os
import pandas as pd
from app import engine
from scripts.utils.common import *
from scripts.utils.deduplicates import resolve_existed_records
from scripts.utils.records import OptimizedRecordsProcessor, prepare_df_for_sql, delete_records
from scripts.utils.match import OptimizedMatchLogProcessor, process_match_log, process_taxon_match, zip_match_log
from scripts.utils.geography import process_geo_batch, geo_keys
from scripts.utils.export import export_records_with_taxon
from scripts.utils.update_version import init_update_session, update_update_version
from scripts.utils.dataset import process_dataset, update_dataset_deprecated
from tqdm import tqdm
from scripts.utils.progress import timer
import atexit
import requests

SOLR_URL = 'http://solr:8983/solr/taxa/select'

def fetch_class_list():
    """從 Solr taxa core 取 taxonRank:class 的所有 scientificName,存成集合。"""
    params = {
        'q': 'taxonRank:class',
        'fl': 'scientificName',
        'rows': 100000,
        'wt': 'json',
    }
    resp = requests.get(SOLR_URL, params=params, timeout=60)
    docs = resp.json().get('response', {}).get('docs', [])
    return {d['scientificName'] for d in docs if d.get('scientificName')}

class_list = fetch_class_list()
unmatched_iconic = set()  # 收集不在 kingdom_list / class_list 的 iconic 值

# ===== 參數 =====
# 直接從單一完整 CSV 檔匯入（取代原本逐頁打 API 的版本）
CSV_PATH = 'observations_api_20260601.csv'
CHUNK_SIZE = 3000  # 每個 chunk 處理的列數（原 API 版為 10 頁 * 300）

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName', 'sourceVernacularName', 'sourceClass', 'sourceKingdom']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'ntuforestry'
rights_holder = '愛自然-臺灣(iNaturalist Taiwan)'
info_id = 1

# 更新紀錄
session = init_update_session(rights_holder)
update_version = session.update_version
current_page = session.current_page  # 此處作為 chunk 索引的續跑 checkpoint
now = session.now
records_processor = session.records_processor
matchlog_processor = session.matchlog_processor
dedup_tracker = session.dedup_tracker

# 更新失敗紀錄
atexit.register(records_processor.export_failed_records,
                f'failed_records_{group}_{info_id}.csv')
atexit.register(matchlog_processor.export_failed_records,
                f'failed_match_logs_{group}_{info_id}.csv')

kingdom_list = ['Plantae', 'Archaea', 'Bacteria', 'Protozoa', 'Chromista', 'Fungi', 'Animalia', 'Viruses', 'Zilligvirae', 'Heunggongvirae', 'Loebvirae', 'Sangervirae', 'Shotokuvirae', 'Trapavirae', 'Orthornavirae', 'Pararnavirae', 'Bamfordvirae', 'Helvetiavirae']

# 全部以字串讀入、空值維持空字串（避免 id / 座標型別跑掉；geography 內部會自行 float() 轉換）
reader = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False, na_values=[], chunksize=CHUNK_SIZE)

start_chunk = current_page  # 續跑：略過已處理過的 chunk
completed = False

for c, df in enumerate(tqdm(reader, unit=' chunk', desc=group)):
    if c < start_chunk:
        continue

    df = df.reset_index(drop=True)
    df = df.replace(to_quote_dict)
    
    # 僅選研究等級的資料
    df = df[df.quality_grade == 'research']
    # CSV 的布林欄位是字串 'TRUE'/'FALSE'，轉成 Python bool
    df['coordinates_obscured'] = df['coordinates_obscured'].map(
        {'TRUE': True, 'true': True, 'FALSE': False, 'false': False}
    ).fillna(False)
    df['sourceKingdom'] = df['iconic_taxon_name'].where(df['iconic_taxon_name'].isin(kingdom_list))
    df['sourceClass'] = df['iconic_taxon_name'].where(df['iconic_taxon_name'].isin(class_list))
    # 兩個清單都不在的 iconic 值 → 記錄下來供事後處理
    unmatched_iconic.update(
        df.loc[
            df['iconic_taxon_name'].notna()
            & ~df['iconic_taxon_name'].isin(kingdom_list)
            & ~df['iconic_taxon_name'].isin(class_list),
            'iconic_taxon_name'
        ].unique()
    )
    df = filter_by_license_and_sensitivity(df)
    if not len(df):
        update_update_version(update_version=update_version, rights_holder=rights_holder,
                                current_page=c + 1, note=None, total_count=records_processor.success_count)
        continue
    df = df.reset_index(drop=True)
    df = df.replace(to_quote_dict)
    df = df.rename(columns={
        'id': 'occurrenceID',
        'user_login': 'recordedBy',
        'created_at': 'sourceCreated',
        'updated_at': 'sourceModified',
        'url': 'references',
        'scientific_name': 'sourceScientificName',
        'common_name': 'sourceVernacularName',
    })
    df['eventDate'] = df.apply(lambda x: x.time_observed_at if x.time_observed_at else x.observed_on, axis=1)
    df['locality'] = df.apply(
        lambda x: ', '.join(filter(None, [x.place_county_name, x.place_country_name])) or None,
        axis=1
    )
    df['basisOfRecord'] = 'HumanObservation'
    df = process_taxon_match(df, sci_cols)
    df = apply_common_fields(df, group, rights_holder, now)
    # 如果sensitiveCategory為重度 只保留年份
    df.loc[df['coordinates_obscured'] == True, ['standardDate', 'month', 'day']] = None
    df.loc[df['coordinates_obscured'] == True, 'eventDate'] = df['year'].astype('Int64').astype(str)
    df = apply_record_type(df, mode='occ')

    # 影像：CSV 的 image_url -> associatedMedia
    df['associatedMedia'] = df['image_url']
    # df['mediaLicense'] = df['license'] # 缺license
    df, media_rule_list = apply_media_rule(df, [])

    # 地理資訊
    # 未模糊化座標看 private_longitude/private_latitude，空值才用 longitude/latitude
    # 依 coordinates_obscured 決定模糊化（True 為重度）
    df['verbatimLatitude'] = df['private_latitude'].where(
        df['private_latitude'].astype(bool), df['latitude']
    )
    df['verbatimLongitude'] = df['private_longitude'].where(
        df['private_longitude'].astype(bool), df['longitude']
    )
    df['sensitiveCategory'] = df['coordinates_obscured'].map({True: '重度'})
    df['dataGeneralizations'] = df['coordinates_obscured'].map({True: True})  # False/其他 → None
    df[geo_keys] = process_geo_batch(df, is_full_hidden='auto')
    df = df.replace(to_quote_dict)
    df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)

    # 資料集
    df['datasetName'] = 'iNaturalist Research-grade Observations'
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
    export_records_with_taxon(df_for_sql, f'/solr/csvs/export/{group}_{info_id}_{c}.csv')
    update_media_rules(media_rules=media_rule_list, rights_holder=rights_holder, now=now)
    timer.batch_summary(label=f"chunk {c}")

    # 處理完一個 chunk 就更新 checkpoint（current_page 紀錄下一個要處理的 chunk）
    update_update_version(update_version=update_version, rights_holder=rights_holder,
                          current_page=c + 1, note=None, total_count=records_processor.success_count)

if unmatched_iconic:
    pd.DataFrame(sorted(unmatched_iconic), columns=['iconic_taxon_name']).to_csv(
        f'unmatched_iconic_{group}_{info_id}.csv', index=False
    )
    print(f'未歸類的 iconic 值 {len(unmatched_iconic)} 個 -> unmatched_iconic_{group}_{info_id}.csv')

completed = True

if completed:
    failed_tbia_ids = {r['tbiaID'] for r in records_processor.failed_records if r.get('tbiaID')}
    delete_records(rights_holder=rights_holder, group=group, update_version=int(update_version), exclude_ids=failed_tbia_ids)
    zip_match_log(group=group, info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder, total_count=records_processor.success_count)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)
    print('done!')