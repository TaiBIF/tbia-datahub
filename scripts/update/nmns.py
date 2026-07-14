import requests
import pandas as pd
import time
import json
from app import engine
from scripts.utils.common import *
from scripts.utils.deduplicates import resolve_existed_records
from scripts.utils.records import prepare_df_for_sql, delete_records
from scripts.utils.match import process_match_log, process_taxon_match, zip_match_log, clean_html_tags
from scripts.utils.geography import process_geo_batch, geo_keys, parse_verbatim_coords
from scripts.utils.export import export_records_with_taxon
from scripts.utils.update_version import init_update_session, update_update_version
from scripts.utils.dataset import process_dataset, update_dataset_deprecated
from tqdm import tqdm
from scripts.utils.progress import timer
import atexit
import os

# 比對學名時使用的欄位
sci_cols = ['sourceScientificName', 'sourceVernacularName', 'sourceOrder', 'sourceFamily', 'sourceClass', 'sourceKingdom']

# 單位資訊 (在portal.Partner.info裡面的id)
group = 'nmns'
rights_holder = '科博典藏 (NMNS Collection)'
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

if not note:
    category_index = 0
    offset = 0
else:
    category_index = note.get('category_index')
    offset = note.get('offset')

# 鳥獸學門 - 沒有資料
# v 鳥獸學門(哺乳類蒐藏) - 中文名 Chinese Common Name, 目中文名, 目名(英文), 科中文名, 科名(英文), 學名(含<i>)
# v 鳥獸學門(鳥類蒐藏) - 中文名 Chinese Common Name, 目中文名, 目名(英文), 科中文名, 科名(英文), 學名(含<i>)
# v 非維管束學門 - 中文名 Chinese Common Name, 類別(會寫是藻類/苔蘚/地衣), 學名 Scientific Name(含<i>), 分類資訊 Taxonomy(門 (Phylum) ， 綱 (Class) ， 目 (Order) ， 科 (Family) 全形逗號分隔 但有可能有前後多餘空格)
# v 昆蟲學門 - 中文名 Chinese Common Name, 目名(英文), 科名(英文), 學名(含<i>), 
# v 無脊椎動物學門 - 中文名 Chinese Common Name, 科名(英文), 學名(含<i>), 中文科名
# v 維管束學門 - 中文名 Chinese Common Name, 科名(英文+空格+中文) 學名, 
# v 真菌學門 - 中文名 Chinese Common Name, 學名 Scientific Name(含<i>), 分類資訊 Taxonomy(門 (Phylum) ， 綱 (Class) ， 目 (Order) ， 科 (Family) 全形逗號分隔 但有可能有前後多餘空格)
# v 兩棲爬蟲學門 - 中文名 Chinese Common Name, 目名 (英文+空格＋中文), 科名(英文+空格＋中文), 屬名(含<i>), 種名(實際上是種小名, 含<i>), 
# v 古生物學門 - 學名 Scientific Name, 名稱 Chinese Common Name, 科名 Family(英文),	目名 Order(英文), 綱名 Class(英文),	門名 Phylum(英文), 界名 Kingdom(英文)
# 兩爬學門(魚類蒐藏) - 沒有資料

# 依據學門給kingdom
kingdom_map = {
    '昆蟲學門': 'Animalia',
    '兩棲爬蟲學門': 'Animalia',
    '無脊椎動物學門': 'Animalia',
    '維管束學門': 'Plantae',
    '真菌學門': 'Fungi',
    '鳥獸學門(哺乳類蒐藏)': 'Animalia',
    '鳥獸學門(鳥類蒐藏)': 'Animalia',
    # 古生物學門靠「界名 Kingdom」欄位帶入，不放這
    # '非維管束學門': None,  <-- 這裡留空，等到迴圈內依據「類別」動態處理
}

category_list = ['昆蟲學門', '兩棲爬蟲學門', '非維管束學門', '維管束學門', '真菌學門',
                 '無脊椎動物學門', '古生物學門', '鳥獸學門(哺乳類蒐藏)', '鳥獸學門(鳥類蒐藏)']

field_map = {
    # --- 學名 ---
    '中文名 Chinese Common Name': 'sourceVernacularName', # 裡面可能有學名 但暫時不處理
    '名稱 Chinese Common Name': 'sourceVernacularName', # 裡面可能有學名 但暫時不處理
    '學名': 'sourceScientificName',
    '學名 Scientific Name': 'sourceScientificName',
    '屬名': 'genus',
    '種名': 'specificEpithet',
    # --- 分類階層 (Kingdom/Phylum/Class 另外邏輯處理) ---
    '目名': 'sourceOrder',
    '科名': 'sourceFamily',
    # '中文科名': '', 統一用英文科名
    # --- 古生物學門專屬階層 (caption 名稱不同，不衝突可直接對應) ---
    '界名 Kingdom': 'sourceKingdom',
    '綱名 Class':   'sourceClass',
    '目名 Order':   'sourceOrder',
    '科名 Family':  'sourceFamily',
    # '門名 Phylum': 'sourcePhylum',  # 現行流程不比對門，需要才開
    # --- 館藏編號 ---
    '館號/編目號 Catalog No.':  'catalogNumber',
    '館號 TNM No.': 'catalogNumber',
    '採集號':   'recordNumber',
    "採集號 Collector's No.":   'recordNumber',
    # --- 人員與時間 ---
    '採集日':   'eventDate',
    '採集日期': 'eventDate',
    '採集日期 Collection Date': 'eventDate',
    '採集者':   'recordedBy',
    '採集者 Collector': 'recordedBy',
    # '鑑定者': ,
    # '鑑定者': ,
    # '鑑定日期': ,
    # --- 地點與環境 ---
    '國名': 'locality_1',
    '行政區域': 'locality_1',
    '省/縣名':  'locality_2',
    '採集地':   'locality_3',
    '採集地名': 'locality_3',
    '採集地 Locality':  'locality_3',
    '產地 Locality': 'locality_3',
    '經緯度 Coordinates': 'Coordinates',
    # '海拔': ,
    # '海拔 Altitude': ,
    # '海拔高度(m)': ,
    # '基質 Substrate': ,
    # --- 標本屬性 ---
    '保存方式': 'preservation',
    '數量': 'organismQuantity',
    '保存液': 'preservation',
    '固定方式': 'preservation',
    '標本類型 Types of specimens': 'typeStatus',
    # '菌株 Culture':
    # 'DNA': 
}

field_list = list(set(field_map.values()))


for now_category in category_list[category_index:]:
    has_more_data = True
    data = []
    all_count = 0
    while has_more_data:
        # 須在server上執行
        url = 'https://collections.culture.tw/getMetadataList.aspx?FORMAT=NMNS&DEPARTMENT={}&LIMIT=100&OFFSET={}'.format(now_category, offset)
        resp = requests.get(url)
        try:
            resp = resp.json()
        except:
            resp = []
        all_count += len(resp)
        if len(resp) < 100:
            has_more_data = False
            batch_tag = offset      # 保留重置前的位置給檔名
            offset = 0
        else:
            offset += 100
            batch_tag = offset
        for r in resp:
            now_dict = {'sourceModified': r.get('MDDATE'), 
                        'references': r.get('CollectionUrl'),
                        'associatedMedia': r.get('ImageList'),
                        'license': r.get('GalCC')}
            # Step A: 先依據學門給予預設的 Kingdom
            if now_category in kingdom_map:
                now_dict['sourceKingdom'] = kingdom_map[now_category]
            # --- 處理 MetaData_NMNS ---
            for rr in r.get('MetaData_NMNS'):
                caption = rr.get('Caption')
                value = rr.get('Value')
                # 1. 一般欄位處理
                if caption in field_map.keys():
                    now_dict[field_map[caption]] = value
                # 2. 特殊欄位：類別 (處理非維管束學門的 Kingdom)
                elif caption == '類別' and now_category == '非維管束學門' and value:
                    if '苔蘚' in value:
                        now_dict['sourceKingdom'] = 'Plantae'
                    elif '地衣' in value:
                        now_dict['sourceKingdom'] = 'Fungi'
                    # 若是藻類，這裡不做動作，保持沒有 sourceKingdom 的狀態
                # 3. 特殊欄位：分類資訊 Taxonomy
                elif caption == '分類資訊 Taxonomy' and value:
                    # 統一處理：全形逗號切割、去除空白、轉首字大寫
                    tax_parts = [x.strip().title() for x in value.split('，')]
                    if now_category == '真菌學門':
                        # 真菌邏輯 (4層)：門, 綱, 目, 科
                        # if len(tax_parts) > 0: now_dict['sourcePhylum'] = tax_parts[0] # 不取門
                        if len(tax_parts) > 1: now_dict['sourceClass'] = tax_parts[1]
                        if len(tax_parts) > 2: now_dict['sourceOrder'] = tax_parts[2]
                        if len(tax_parts) > 3: now_dict['sourceFamily'] = tax_parts[3]
                    elif now_category == '非維管束學門': 
                        # 藻類邏輯 (3層)：門, 目, 科 (跳過綱)
                        # if len(tax_parts) > 0: now_dict['sourcePhylum'] = tax_parts[0] # 不取門
                        if len(tax_parts) > 1: now_dict['sourceOrder'] = tax_parts[1]
                        if len(tax_parts) > 2: now_dict['sourceFamily'] = tax_parts[2]
            data.append(now_dict)
        if len(data) > 9999 or not has_more_data:
            print(now_category, len(data), all_count)
            df = pd.DataFrame(data)
            # 存原始抓回資料供最後核對筆數 每個 batch 各自存一個檔案，避免跨 batch 欄位不一致造成錯位
            raw_path = f'/solr/csvs/raw_{group}_{info_id}_{now_category}_{batch_tag}.csv'
            df.to_csv(raw_path, index=False, encoding='utf-8-sig')
            df = df.replace(to_quote_dict)
            data = [] # 重新下一個loop
            # 先補上所有欄位 避免後面錯誤
            new_columns = df.columns.union(field_list, sort=False)
            df = df.reindex(columns=new_columns, fill_value='')
            # 全欄位清 HTML tag（含 <i>），順便 strip 掉古生物階層的前後空白
            df = df.apply(lambda col: col.map(clean_html_tags) if col.dtype == 'object' else col)
            # genus + specificEpithet 只作為 fallback，避免覆蓋已有的學名
            binomial = (df['genus'].fillna('').str.strip() + ' ' +
                        df['specificEpithet'].fillna('').str.strip()).str.strip()
            sci = df['sourceScientificName'].fillna('').str.strip()
            df['sourceScientificName'] = sci.where(sci != '', binomial)
            df = filter_by_license_and_sensitivity(df)
            if len(df):
                df = df.reset_index(drop=True)
                df = df.replace(to_quote_dict)
                # locality 合併
                locality_cols = [c for c in ['locality', 'locality_1', 'locality_2', 'locality_3'] if c in df.columns]
                df['locality'] = df[locality_cols].apply(
                    lambda row: ' '.join(v.strip() for v in row if isinstance(v, str) and v.strip()),
                    axis=1
                )
                # associatedMedia：ImageList 併成分號分隔字串
                if 'associatedMedia' in df.columns:
                    df['associatedMedia'] = df['associatedMedia'].apply(
                        lambda x: ';'.join(x) if isinstance(x, list) else (x or '')
                    )
                    # mediaLicense 數量對齊 associatedMedia（每張一個 OGDL）
                    df['mediaLicense'] = df['associatedMedia'].map(
                        lambda x: ';'.join(['OGDL'] * len(x.split(';'))) if x else None
                    )
                df = process_taxon_match(df, sci_cols)
                df = apply_common_fields(df, group, rights_holder, now)
                df = apply_record_type(df, mode='col')  # basisOfRecord 無資料
                df, media_rule_list = apply_media_rule(df, [])
                # 地理資訊
                # 目前僅有非維管束學門有經緯度(真菌學門可能也有) 並且沒有敏感資料
                if 'Coordinates' in df.columns:
                    df[['verbatimLatitude', 'verbatimLongitude']] = df['Coordinates'].apply(
                        lambda x: pd.Series(parse_verbatim_coords(x))
                    )
                    df[geo_keys] = process_geo_batch(df, skip_blur=True)
                df = df.replace(to_quote_dict)
                df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
                # 資料集
                df['datasetName'] = rights_holder + '-' + now_category
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
                process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=now_category)
                export_records_with_taxon(
                    df_for_sql,
                    f'/solr/csvs/export/{group}_{info_id}_{now_category}_{batch_tag}.csv'
                )
                update_media_rules(media_rules=media_rule_list,rights_holder=rights_holder, now=now)
        # 成功之後 更新update_update_version
        update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=None, note=json.dumps({'category_index': category_index, 'offset': offset}),total_count=records_processor.success_count)
    category_index += 1
    offset = 0
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=None, note=json.dumps({'category_index': category_index, 'offset': offset}),total_count=records_processor.success_count)

if not has_more_data:
    failed_tbia_ids = {r['tbiaID'] for r in records_processor.failed_records if r.get('tbiaID')}
    delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version),exclude_ids=failed_tbia_ids)
    # delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
    zip_match_log(group=group,info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder,total_count=records_processor.success_count)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)


print('done!')