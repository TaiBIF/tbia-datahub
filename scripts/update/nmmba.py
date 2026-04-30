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
group = 'nmmba'
rights_holder = '國立海洋生物博物館生物典藏管理系統'
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

token = os.getenv('NMMBA_TOKEN')
headers = {"Authorization": f"Bearer {token}"}
c = current_page if current_page != 0 else 1
has_more_data = True
should_stop = False


import re

# 1. [核心邏輯] 判斷並拆分逗號混合的座標
def resolve_mixed_comma(row):
    """
    輸入一行資料，檢查 longitude_start 是否含有逗號。
    若有，則拆分並判斷經緯度歸位；若無，則回傳原值。
    回傳順序固定為: lon_start, lon_end, lat_start, lat_end
    """
    l_start = str(row['longitude_start']).strip()
    # 如果沒有逗號，直接回傳原本的四個欄位，不做更動
    if ',' not in l_start:
        return row['longitude_start'], row['longitude_end'], row['latitude_start'], row['latitude_end']
    # 處理含有逗號的情況
    parts = [p.strip() for p in l_start.split(',')]
    # 預設回傳值 (萬一解析失敗則回傳原值)
    if len(parts) != 2:
        return row['longitude_start'], row['longitude_end'], row['latitude_start'], row['latitude_end']
    p1, p2 = parts[0], parts[1]
    # --- 判斷 p1 是緯度還是經度 ---
    p1_is_lat = False
    p1_u = p1.upper()
    # A. 先看方向標示
    if 'N' in p1_u or 'S' in p1_u:
        p1_is_lat = True
    elif 'E' in p1_u or 'W' in p1_u:
        p1_is_lat = False
    else:
        # B. 若無方向，看數值 (假設 <= 90 為緯度)
        nums = re.findall(r"(\d+(?:\.\d*)?|\.\d+)", p1)
        if nums and float(nums[0]) <= 90:
            p1_is_lat = True
    # --- 歸位並清空 End 欄位 ---
    if p1_is_lat:
        return p2, np.nan, p1, np.nan  # lon=p2, lat=p1
    else:
        return p1, np.nan, p2, np.nan  # lon=p1, lat=p2


# 2. [核心邏輯] 合併 Start/End (您原本的函式，稍微縮減寫法)
def format_merge_text(start, end):
    """
    合併 Start 與 End 欄位。
    若 Start == End 或 End 為空，只回傳 Start。
    否則回傳 Start-End。
    """
    s = str(start).strip() if pd.notna(start) and str(start).lower() != 'nan' else ''
    e = str(end).strip() if pd.notna(end) and str(end).lower() != 'nan' else ''
    if not s:
        return ''
    if not e or s == e:
        return s
    return f"{s}-{e}"


# 3. [主流程] 封裝成一個清理函式
def process_coordinate_cleaning(df):
    df_clean = df.copy()
    # 步驟一：處理逗號錯置 (一次更新四個欄位)
    # 使用 apply 搭配 result_type='expand' 可以直接展開成四個欄位
    cols_to_fix = ['longitude_start', 'longitude_end', 'latitude_start', 'latitude_end']
    df_clean[cols_to_fix] = df_clean.apply(resolve_mixed_comma, axis=1, result_type='expand')
    # 步驟二：處理 Start/End 合併
    df_clean['verbatimLongitude'] = df_clean.apply(
        lambda row: format_merge_text(row['longitude_start'], row['longitude_end']), axis=1
    )
    df_clean['verbatimLatitude'] = df_clean.apply(
        lambda row: format_merge_text(row['latitude_start'], row['latitude_end']), axis=1
    )
    return df_clean

while has_more_data:
    data = []
    p = c + 10
    while c < p and has_more_data:
        # offset = 300 * c
        time.sleep(1)
        url = f"https://helloocean.nmmba.gov.tw/nmmba_front/API/get.aspx?pageIndex={c}"
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            result = response.json()
            total_page = result['totalPages']
            now_mediaLicense = result['mediaLicense']
            now_license = result['license']
            data += result.get('data')
            print('page:',c , 'total page:', total_page)
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
        # 如果學名相關的欄位都是空值才排除
        df['license'] = now_license
        df['mediaLicense'] = now_mediaLicense
        df['sourceScientificName'] = df.apply(lambda x: x.genus_en + ' ' + x.species_en, axis=1)
        # 如果學名相關的欄位都是空值才排除
        df = filter_by_taxon_fields(df, required_cols=['sourceScientificName','species_ch','family_en'])
        df = filter_by_license_and_sensitivity(df)
        if len(df):
            df = df.reset_index(drop=True)
            df = df.replace(to_quote_dict)
            df = df.rename(columns={'nmmba_id': 'catalogNumber', # 館藏號
                                'id': 'reference_id', # 館藏號
                                'quantity': 'organismQuantity',
                                'collect_date': 'eventDate',
                                'family_en': 'sourceFamily',
                                'species_ch': 'sourceVernacularName', 
                                'mode_ch': 'typeStatus',
                                'created_at': 'sourceCreated',
                                'modified_at': 'sourceModified',
                                'preserve_ch': 'preservation',
                                'quantity': 'organismQuantity',
                                'associatedMedias': 'associatedMedia',
                                }
                        ) 
            df['locality'] = df['location_ch'] + ' ' + df['location_en']
            df['recordedBy'] = df['collector_ch'] + ' ' + df['collector_en']
            # 經緯度預處理
            df = process_coordinate_cleaning(df)
            df['references'] = "https://helloocean.nmmba.gov.tw/nmmba_front/SpecimenDetail.aspx?id=" + df['reference_id'].astype(str)   
            if 'associatedMedia' in df.columns:
                df['associatedMedia'] = df['associatedMedia'].apply(lambda x: (';').join(['https://helloocean.nmmba.gov.tw/nmmba_front/SpecimenPicture/' + xx.get('name') for xx in x]))
            df = process_taxon_match(df, sci_cols)
            df = apply_common_fields(df, group, rights_holder, now)
            df = apply_record_type(df, mode='col')  # basisOfRecord 無資料
            df, media_rule_list = apply_media_rule(df, [])
            df[geo_keys] = process_geo_batch(df, skip_blur=True) # 敏感層級 無資料
            df = df.replace(to_quote_dict)
            df['dataQuality'] = df.apply(lambda x: calculate_data_quality(x), axis=1)
            df['datasetName'] = '國立海洋生物博物館生物典藏管理系統' # 幫忙補
            df = process_dataset(df, group, rights_holder, update_version, now)
            df, existed_records = resolve_existed_records(df, rights_holder, dedup_tracker)
            df = df.replace(to_none_dict)
            process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=c)
            df = prepare_df_for_sql(df, update_version)
            records_processor.smart_upsert_records(df, existed_records=existed_records)
            export_records_with_taxon(df, f'/solr/csvs/export/{group}_{info_id}_{c}.csv')
            update_media_rules(media_rules=media_rule_list,rights_holder=rights_holder, now=now)
    # 成功之後 更新update_update_version
    update_update_version(update_version=update_version, rights_holder=rights_holder, current_page=c, note=None)

if not has_more_data:
    delete_records(rights_holder=rights_holder,group=group,update_version=int(update_version))
    zip_match_log(group=group,info_id=info_id)
    update_update_version(is_finished=True, update_version=update_version, rights_holder=rights_holder)
    update_dataset_deprecated(rights_holder=rights_holder, update_version=update_version)
    records_processor.export_failed_records(f'failed_records_{group}_{info_id}.csv')
    matchlog_processor.export_failed_records(f'failed_match_logs_{group}_{info_id}.csv')


print('done!')






# >>> df.mode_ch.unique()
# array(['一般標本', '副模標本', '珍稀標本', '一般展示品', '保育類展示品(大型 >50cm)', '保育類展示品', '',
#        '一般展示品(大型 >50cm)', '正模標本', '新模標本', '副選模標', '選模標本', '一般教具'],
#       dtype=object)

# >>> df.status_ch.unique()
# array(['已上架', '全部借出', '待入庫', '展示中', '未尋獲', '其它', '註銷', '部分借出', '贈送', '交換',
#        '遺失'], dtype=object)

# >>> df.country_ch.unique()
# array(['台灣', '美國', '日本', '巴西', '越南', '', '印尼', '中國大陸', '羅馬尼亞', '澳大利亞',
#        '泰國', '印度', '香港', '菲律賓', '阿拉伯聯合大公國', '馬來西亞', '埃及', '新加坡', '萬那度',
#        '東非', '吉里巴斯', '紐西蘭', '新喀里多尼亞', '斐濟', '馬紹爾群島', '坦尚尼亞', '馬爾地夫',
#        '南極洲', '寮國', '墨西哥', '肯亞', '南非', '蓋亞那', '蘇利南', '巴拿馬', '委內瑞拉',
#        '哥倫比亞', '厄瓜多爾', '秘魯', '多明尼加', '俄羅斯', '緬甸', '義大利', '未知', '千里達及托貝哥',
#        '沙烏地阿拉伯', '孟加拉', '韓國', '巴哈馬', '智利', '聖文森及格瑞那丁', '加拿大', '馬達加斯加'],
#       dtype=object)

