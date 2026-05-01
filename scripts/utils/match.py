# 學名比對相關
import requests
import numpy as np
import pandas as pd
import urllib.parse
import concurrent.futures
import time
import re
import json
import pymysql
import os
import subprocess
from sqlalchemy import text
from dotenv import load_dotenv
load_dotenv(override=True)


taicol_db_settings = {
    "host": os.getenv('TaiCOL_DB_HOST'),
    "port": int(os.getenv('TaiCOL_DB_PORT')),
    "user": os.getenv('TaiCOL_DB_USER'),
    "password": os.getenv('TaiCOL_DB_PASSWORD'),
    "database": os.getenv('TaiCOL_DB_DBNAME'),
}


def clean_html_tags(text):
    # 1. 檢查是否為字串 (處理 NaN 或 float 的情況)
    if not isinstance(text, str):
        return text
    # 2. 移除 <i> 和 </i>
    # 3. 移除前後多餘空白 (.strip())
    return text.replace('<i>', '').replace('</i>', '').strip()

match_cols = ['taxonID','sci_index',
                'match_stage', 'match_higher_taxon', 'stage_1', 'stage_2', 'stage_3',
                'stage_4', 'stage_5', 'stage_6', 'stage_7', 'stage_8']

deleted_taxon_ids = pd.read_csv('/bucket/deleted_taxon.csv')
deleted_taxon_ids = deleted_taxon_ids.taxon_id.to_list()


match_issue_map = {
    1: 'higherrank',
    2: 'none',
    3: 'fuzzy',
    4: 'multiple'
}

rank_map = {
    1: 'Domain', 2: 'Superkingdom', 3: 'Kingdom', 4: 'Subkingdom', 5: 'Infrakingdom', 6: 'Superdivision', 7: 'Division', 8: 'Subdivision', 9: 'Infradivision', 10: 'Parvdivision', 11: 'Superphylum', 12:
    'Phylum', 13: 'Subphylum', 14: 'Infraphylum', 15: 'Microphylum', 16: 'Parvphylum', 17: 'Superclass', 18: 'Class', 19: 'Subclass', 20: 'Infraclass', 21: 'Superorder', 22: 'Order', 23: 'Suborder',
    24: 'Infraorder', 25: 'Superfamily', 26: 'Family', 27: 'Subfamily', 28: 'Tribe', 29: 'Subtribe', 30: 'Genus', 31: 'Subgenus', 32: 'Section', 33: 'Subsection', 34: 'Species', 35: 'Subspecies', 36:
    'Nothosubspecies', 37: 'Variety', 38: 'Subvariety', 39: 'Nothovariety', 40: 'Form', 41: 'Subform', 42: 'Special Form', 43: 'Race', 44: 'Stirp', 45: 'Morph', 46: 'Aberration', 47: 'Hybrid Formula'}


def process_taxon_match(df, sci_cols):
    """
    從 df 取出學名欄位去重後比對 TaiCOL，並將結果 merge 回 df。

    Args:
        df: 原始資料 DataFrame
        sci_cols: 比對學名使用的欄位（需包含 'taxonID'）

    Returns:
        df: 已加上 match_cols 比對結果的 DataFrame
    """
    missing = [c for c in sci_cols if c not in df.columns]
    if missing:
        df = df.assign(**{c: '' for c in missing})
    sci_names = df[sci_cols].drop_duplicates().reset_index(drop=True)
    sci_names['sci_index'] = sci_names.index
    df = df.merge(sci_names)
    match_results = matching_flow_new_optimized(sci_names)
    df = df.drop(columns=['taxonID'], errors='ignore')
    if len(match_results):
        df = df.merge(match_results[match_cols], on='sci_index', how='left')
    return df


def get_namecode(namecode):
    conn = pymysql.connect(**taicol_db_settings)
    with conn.cursor() as cursor:     
        query = """
        WITH cte
            AS
            (
                SELECT distinct anc.namecode, anc.taxon_name_id, atu.taxon_id, atu.status, at.is_deleted
                FROM api_namecode anc
                LEFT JOIN api_taxon_usages atu ON atu.taxon_name_id = anc.taxon_name_id
                LEFT JOIN api_taxon at ON at.taxon_id = atu.taxon_id
                WHERE anc.namecode = %s
            )
        SELECT namecode, taxon_name_id, 
        JSON_ARRAYAGG(JSON_OBJECT('taxon_id', taxon_id, 'status', status, 'is_deleted', is_deleted))
        FROM cte GROUP BY namecode, taxon_name_id;
        """
        cursor.execute(query, (namecode))
        df = pd.DataFrame(cursor.fetchall(), columns=['namecode', 'name_id', 'taxon'])
        for i in df.index:
            row = df.iloc[i]
            taxon_tmp = json.loads(row.taxon)
            taxon_final = []
            for t in taxon_tmp:
                if t.get('is_deleted'):
                    taxon_final.append({'taxon_id': t.get('taxon_id'), 'usage_status': 'deleted'})
                elif t.get('taxon_id'):
                    taxon_final.append({'taxon_id': t.get('taxon_id'), 'usage_status': t.get('status')})
            df.loc[i,'taxon'] = json.dumps(taxon_final)
        if len(df):
            df['taxon'] = df['taxon'].replace({np.nan:'[]'})
            df['taxon'] = df['taxon'].apply(json.loads)
        return df.to_dict('records')


def match_namecode(matching_namecode, match_stage, sci_names, sci_index):
    # 這邊不會有fuzzy的問題 因為直接用namecode對應
    # 也不考慮高階層
    try:
        matching_namecode = str(int(matching_namecode))
    except:
        pass
    # 改成用TaiCOL API
    # taxon_name_id = None
    taxon_data = []
    if name_data := get_namecode(matching_namecode):
        name_data = name_data[0] # 一個 namecode 對應一個 taxon_name_id
        taxon_data = pd.DataFrame(name_data.get('taxon'), columns=['taxon_id','usage_status'])
        # 不用排除誤用，但優先序為 有效 -> 無效 -> 誤用
        # 如果有accepted，僅考慮accepted
        if len(taxon_data[taxon_data.usage_status=='accepted']):
            taxon_data = taxon_data[taxon_data.usage_status=='accepted']
        # 如果沒有accepted，但有not-accepted，僅考慮not-accepted
        elif len(taxon_data[taxon_data.usage_status=='not-accepted']):
            taxon_data = taxon_data[taxon_data.usage_status=='not-accepted']
        taxon_data = taxon_data.taxon_id.to_list()
        if len(taxon_data) == 1:
            sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = taxon_data[0]
            sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = None
        else:
            sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 4


def matching_flow_new_optimized(sci_names, batch_size=50, max_workers=4):
    """
    優化版本的 matching_flow_new
    
    主要改進：
    1. 批次大小從20增加到100
    2. 並行處理多個API請求  
    3. 減少重複的DataFrame操作
    4. 向量化字符串處理
    5. 早期退出機制（已匹配的不再處理）
    """
    
    def batch_match_names(names_list, is_parent, match_stage, specific_rank):
        """批次處理名稱匹配"""
        if not names_list:
            return []

        # 檢查 URL 長度，太長就拆半遞迴
        joined = urllib.parse.quote('|'.join(names_list))
        if len(joined) > 7000 and len(names_list) > 1:
            mid = len(names_list) // 2
            return (batch_match_names(names_list[:mid], is_parent, match_stage, specific_rank) +
                    batch_match_names(names_list[mid:], is_parent, match_stage, specific_rank))
  
        try:
            request_url = f"http://host.docker.internal:8080/api.php?names={urllib.parse.quote(('|').join(names_list))}&format=json&source=taicol"
            response = requests.get(request_url, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                processed_results = []
                
                # 確保 result['data'] 存在且是 list
                if 'data' not in result or not isinstance(result['data'], list):
                    return []
                
                # 處理每個名稱的結果
                for i, r in enumerate(result['data']):
                    # r 應該是一個 list，包含該名稱的匹配結果
                    if r and isinstance(r, list) and len(r) > 0:
                        # 取第一個（最佳）匹配結果
                        best_match = r[0]
                        
                        if isinstance(best_match, dict):
                            search_term = best_match.get('search_term', names_list[i] if i < len(names_list) else '')
                            api_results = best_match.get('results', [])
                            
                            # 確保 results 是 list
                            if isinstance(api_results, list):
                                processed_results.append({
                                    'search_term': search_term,
                                    'results': api_results
                                })
                
                return processed_results
                
        except Exception as e:
            print(f"API request error: {e}")
            
        return []
    
    def process_stage_vectorized(sci_names, stage_num, column_name, is_parent=False, 
                               specific_rank=None, transform_func=None):
        """向量化處理單個匹配階段，確保 stage_* 有正確資訊"""
        
        # 早期退出：如果所有記錄都已經有taxonID，跳過
        if (sci_names['taxonID'] != '').all():
            return sci_names
        
        # 設定當前階段
        sci_names.loc[sci_names.taxonID == '', 'match_stage'] = stage_num
        
        # 選擇需要處理的記錄
        no_taxon = sci_names[sci_names.taxonID == ''].copy()
        
        if column_name not in no_taxon.columns:
            # 如果欄位不存在，跳過此階段
            # print(f"  Stage {stage_num}: Column '{column_name}' not found - skipping")
            return sci_names
            
        if no_taxon[column_name].isna().all():
            # 如果欄位存在但全部為空，跳過此階段
            # print(f"  Stage {stage_num}: No data in column '{column_name}'")
            return sci_names
        
        # 向量化清理數據
        no_taxon['now_matching_name'] = no_taxon[column_name].fillna('').astype(str)
        
        # 清理 HTML tags
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].apply(clean_html_tags)

        # 應用轉換函數（如果有）
        if transform_func:
            no_taxon['now_matching_name'] = transform_func(no_taxon['now_matching_name'])
        
        # 過濾空值
        matching_df = no_taxon[no_taxon.now_matching_name != ''].copy()
        
        # 如果沒有有效的匹配名稱，保持與原版一致，不設置stage值
        if matching_df.empty:
            # print(f"  Stage {stage_num}: No valid names to match")
            return sci_names
        
        # print(f"  Stage {stage_num}: Processing {len(matching_df)} names")
        
        # 選擇需要的欄位
        keep_columns = ['now_matching_name', 'sci_index'] + [
            col for col in ['sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom'] 
            if col in matching_df.columns
        ]
        matching_df = matching_df[keep_columns]
        
        # 向量化處理分號分隔的名稱
        if ';' in matching_df['now_matching_name'].str.cat(sep=''):
            matching_df = matching_df.assign(
                now_matching_name=matching_df['now_matching_name'].str.split(';')
            ).explode('now_matching_name').reset_index(drop=True)
            matching_df = matching_df[matching_df.now_matching_name.str.strip() != '']
        
        # 批次處理API請求
        results = []
        unique_names = matching_df['now_matching_name'].unique()
        
        # 使用線程池並行處理大批次
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for i in range(0, len(unique_names), batch_size):
                batch_names = unique_names[i:i+batch_size].tolist()
                future = executor.submit(
                    batch_match_names, batch_names, is_parent, stage_num, specific_rank
                )
                futures.append((future, batch_names))
            
            # 收集結果
            for future, batch_names in futures:
                try:
                    batch_results = future.result()
                    results.extend(batch_results)
                except Exception as e:
                    print(f"Batch processing error: {e}")
        
        # 處理API結果
        if results:
            results_df = pd.DataFrame(results)
            merged_data = matching_df.merge(results_df, left_on='now_matching_name', right_on='search_term', how='left')
            
            # 向量化處理匹配結果
            successful_matches = _update_sci_names_vectorized(merged_data, sci_names, stage_num, is_parent, specific_rank)
            
            # 保持與原版一致：沒有找到匹配的記錄不設置stage值
            # 原版的行為是保持stage_*為None，不主動標記為'none'
            
            # print(f"    Successful matches: {successful_matches}")
            
        # else:
            # 如果API沒有回傳任何結果，保持與原版一致，不設置stage值
            # print(f"  Stage {stage_num}: No API results")
        
        return sci_names
    
    def _update_sci_names_vectorized(merged_data, sci_names, stage_num, is_parent, specific_rank):
        """向量化更新sci_names，返回成功匹配的數量"""
        
        successful_matches = 0
        
        for idx, row in merged_data.iterrows():
            # 跳過已經有taxonID的記錄
            sci_idx = row['sci_index']
            if (sci_names.loc[sci_names.sci_index == sci_idx, 'taxonID'] != '').any():
                continue
            
            filtered_rs = row.get('results')
            
            # 確保 filtered_rs 是 list 類型
            if not isinstance(filtered_rs, list):
                # print(f"Warning: filtered_rs is not a list, got {type(filtered_rs)}: {filtered_rs}")
                continue
                
            if not filtered_rs:
                # 沒有API結果
                continue
            
            # 向量化過濾高分結果，確保每個元素都是 dict
            try:
                filtered_rs = [fr for fr in filtered_rs if isinstance(fr, dict) and fr.get('score', 0) > 0.7]
            except Exception as e:
                print(f"Error filtering results for sci_index {sci_idx}: {e}")
                print(f"filtered_rs content: {filtered_rs}")
                continue
            
            if not filtered_rs:
                # 沒有高分結果
                continue
            
            # 轉換為DataFrame進行向量化操作
            results_df = pd.DataFrame(filtered_rs)
            required_cols = ['accepted_namecode', 'family', 'order', 'class', 'kingdom', 'name_status', 'score', 'taxon_rank']
            missing_cols = [col for col in required_cols if col not in results_df.columns]
            for col in missing_cols:
                results_df[col] = None
            
            results_df = results_df[required_cols].drop_duplicates()
            
            # 應用rank過濾
            if specific_rank:
                results_df = results_df[results_df.taxon_rank == specific_rank]
            
            if results_df.empty:
                # 沒有符合rank要求的結果
                continue
            
            # 向量化狀態過濾
            if (results_df.name_status == 'accepted').any():
                results_df = results_df[results_df.name_status == 'accepted']
            elif (results_df.name_status == 'not-accepted').any():
                results_df = results_df[results_df.name_status == 'not-accepted']
            
            if results_df.empty:
                # 沒有有效狀態的結果
                continue
            
            # 處理階層匹配
            results_dict = results_df.drop(columns=['name_status', 'taxon_rank']).to_dict(orient='records')
            
            # 向量化階層檢查
            matched_results = _check_hierarchy_match_vectorized(results_dict, row)
            
            if matched_results:
                # 更新sci_names
                best_match = matched_results[0]
                sci_names.loc[sci_names.sci_index == sci_idx, 'taxonID'] = best_match['accepted_namecode']
                
                if is_parent:
                    sci_names.loc[sci_names.sci_index == sci_idx, 'match_higher_taxon'] = True
                    sci_names.loc[sci_names.sci_index == sci_idx, f'stage_{stage_num}'] = 1  # higherrank
                else:
                    # 檢查匹配分數決定 stage 值
                    match_score = best_match.get('score', 0)
                    
                    # 檢查是否為中文
                    is_chinese = bool(re.findall(r'[\u4e00-\u9fff]+', str(row.get('now_matching_name', ''))))
                    if is_chinese and match_score == 0.95:
                        match_score = 1.0
                    
                    if match_score < 1.0:
                        sci_names.loc[sci_names.sci_index == sci_idx, f'stage_{stage_num}'] = 3  # fuzzy
                    else:
                        sci_names.loc[sci_names.sci_index == sci_idx, f'stage_{stage_num}'] = None  # perfect match
                
                successful_matches += 1
                
            elif len(results_dict) > 1:
                # 有多個結果但階層不匹配
                sci_names.loc[sci_names.sci_index == sci_idx, f'stage_{stage_num}'] = 4  # multiple
            # else: 保持與原版一致，不設置stage值
        
        return successful_matches
    
    def _check_hierarchy_match_vectorized(results_dict, row):
        """向量化階層檢查"""
        if not results_dict:
            return []
        
        # 獲取上階層資訊
        source_family = row.get('sourceFamily')
        source_class = row.get('sourceClass') 
        source_order = row.get('sourceOrder')
        source_kingdom = row.get('sourceKingdom')
        
        # 如果沒有上階層資訊，直接返回所有結果
        if not any([source_family, source_class, source_order, source_kingdom]):
            return results_dict
        
        # 過濾匹配的階層
        matched = []
        for result in results_dict:
            if (not source_family or result.get('family') == source_family) and \
               (not source_class or result.get('class') == source_class) and \
               (not source_order or result.get('order') == source_order) and \
               (not source_kingdom or result.get('kingdom') == source_kingdom):
                matched.append(result)
        
        return matched if matched else results_dict
    
    # 主要處理流程
    # print("=== Optimized Matching Flow ===")
    
    # 初始化必要欄位
    if 'taxonID' not in sci_names.columns:
        sci_names['taxonID'] = ''
    if 'sci_index' not in sci_names.columns:
        sci_names['sci_index'] = sci_names.index

    # 2026-03 若資料庫提供的taxonID已經在TaiCOL被刪除，將taxonID改為空值
    sci_names['taxonID'] = sci_names['taxonID'].apply(lambda x: '' if x in deleted_taxon_ids else x)

    sci_names['match_stage'] = 0
    sci_names['match_higher_taxon'] = False
    
    # 初始化所有 stage 欄位
    for i in range(1, 9):
        sci_names[f'stage_{i}'] = None
    
    # 定義處理階段 - 使用正確的欄位名稱
    stages = [
        (1, 'sourceScientificName', False, None, None),
        (3, 'sourceVernacularName', False, None, None),
        (4, 'sourceScientificName', True, 'genus', lambda x: x.str.split(' ').str[0]),
        (5, 'originalVernacularName', False, None, None),
        (6, 'sourceFamily', True, 'family', None),
        (7, 'sourceOrder', True, 'order', None),
        (8, 'sourceClass', True, 'class', None),
    ]
    
    # 處理 Stage 1 - sourceScientificName
    stage_start = time.time()
    sci_names = process_stage_vectorized(sci_names, 1, 'sourceScientificName', False, None, None)
    # print(f"Stage 1: {time.time() - stage_start:.2f}s")
    
    # 處理 Stage 2 (namecode matching) - 保持原有邏輯
    stage2_start = time.time()
    sci_names.loc[sci_names.taxonID == '', 'match_stage'] = 2
    no_taxon = sci_names[sci_names.taxonID == '']
    
    # 檢查是否存在 scientificNameID 欄位
    if 'scientificNameID' in no_taxon.columns:
        # 向量化處理 scientificNameID
        namecode_mask = (no_taxon['scientificNameID'].notna() & 
                         (no_taxon['scientificNameID'] != ''))
        
        if namecode_mask.any():
            namecode_df = no_taxon[namecode_mask].copy()
            # print(f"    Processing {len(namecode_df)} namecode matches...")
            
            for idx, row in namecode_df.iterrows():
                try:
                    # 需要依賴原版的 match_namecode 函數
                    match_namecode(
                        matching_namecode=row['scientificNameID'],
                        match_stage=2,
                        sci_names=sci_names,
                        sci_index=row['sci_index']
                    )
                except:
                    pass  # 忽略namecode匹配錯誤
        # else:
        #     print("    No valid scientificNameID to process")
    # else:
    #     print("    No scientificNameID column found - skipping Stage 2")
    
    # print(f"Stage 2 (namecode): {time.time() - stage2_start:.2f}s")
    
    # 處理 Stage 3-8
    for stage_num, column, is_parent, rank, transform_func in stages[1:]:  # 跳過已處理的stage_1
        stage_start = time.time()
        sci_names = process_stage_vectorized(sci_names, stage_num, column, is_parent, rank, transform_func)
        # print(f"Stage {stage_num}: {time.time() - stage_start:.2f}s")
    
    # 最終清理階段（與原版一致）
    stage_list = [1,2,3,4,5,6,7,8]
    for i in stage_list[:7]:
        for stg in stage_list[stage_list.index(i)+1:]:
            sci_names.loc[sci_names.match_stage==i,f'stage_{stg}'] = None
    
    # 代表比對到最後還是沒有對到
    sci_names.loc[(sci_names.match_stage==8)&(sci_names.taxonID==''),'match_stage'] = None
    
    # print("=== Matching Flow Completed ===")
    return sci_names


def create_match_log_df(match_log, now):
    match_log = match_log.replace({np.nan: None})
    match_log['is_matched'] = match_log['taxonID'].notna()
    match_log['match_higher_taxon'] = match_log['match_higher_taxon'].replace({None: False, '': False})
    match_log['match_stage'] = match_log['match_stage'].apply(lambda x: int(x) if x or x == 0 else None)

    for i in range(1, 9):
        col = f'stage_{i}'
        match_log[col] = match_log[col].apply(lambda x: match_issue_map[x] if x else x)

    match_log[['created', 'modified']] = now
    return match_log.rename(columns={'id': 'tbiaID', 'rightsHolder': 'rights_holder'})

    
def process_match_log(df, matchlog_processor, existed_records, now, group, info_id, suffix=None):
    """建立 match_log、upsert 到 DB、輸出 CSV。

    Args:
        suffix: checkpoint 識別碼字串(如 f"{url_index}_{c}"、c、now_category)。
                None 表示無 pagination，檔名為 {group}_{info_id}.csv。
    模式 A 純頁數(ascdc/asiz/fact 等)
    process_match_log(..., suffix=c)

    模式 B dataset_list(cpc/gbif/ntm/taibif/wra)
    process_match_log(..., suffix=f"{d_list_index}_{c}")

    模式 C cursor(tbn)
    process_match_log(..., suffix=f"{url_index}_{c}")

    模式 D category(nmns)
    process_match_log(..., suffix=now_category)

    無 pagination(nodass/oca)
    process_match_log(...)  # suffix 省略

    """
    match_log_cols = ['occurrenceID','catalogNumber','id','sourceScientificName','taxonID','match_higher_taxon','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','stage_6','stage_7','stage_8','group','rightsHolder','created','modified']
    match_log = create_match_log_df(df[match_log_cols].reset_index(drop=True), now)
    matchlog_processor.smart_upsert_match_log(match_log, existed_records=existed_records)
    filename = f'{group}_{info_id}_{suffix}.csv' if suffix is not None else f'{group}_{info_id}.csv'
    match_log.to_csv(f'/portal/media/match_log/{filename}', index=None)
    # return match_log


def zip_match_log(group, info_id):
    zip_file_path = f'/portal/media/match_log/{group}_{info_id}_match_log.zip'
    csv_file_path = f'{group}_{info_id}*.csv'
    commands = f"cd /portal/media/match_log/; zip -j {zip_file_path} {csv_file_path}; rm {csv_file_path}"
    process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 等待檔案完成
    a = process.communicate()
    return a


class OptimizedMatchLogProcessor:
    """最佳化的 MatchLog 處理器"""
    
    def __init__(self, db_engine, batch_size=300):
        self.db = db_engine
        self.batch_size = batch_size
        self.failed_match_logs = []
        
    def _safe_dollar_quote(self, value):
        """安全的 PostgreSQL dollar quoting，處理 $v$、\0"""
        s = str(value)
        s = s.replace('\0', '')           # 移除 null byte
        s = s.replace('$v$', '$v$$v$')    # 跳脫 dollar quote tag
        return f"$v${s}$v$"

    def smart_upsert_match_log(self, match_log_df, existed_records=None):
        """
        最佳化的 MatchLog 處理
        使用已取得的existed_records判斷，避免重複查詢
        
        Args:
            match_log_df: 要處理的 match_log 資料
            existed_records: 已存在的記錄(從get_existed_records_optimized取得)
        """
        if match_log_df.empty:
            return
            
        # print(f"🎯 Processing {len(match_log_df)} match_log records...")
        start_time = time.time()
        
        # 1. 使用已取得的existed_records判斷（避免重複查詢）
        if existed_records is not None and not existed_records.empty:
            existing_ids = set(existed_records['tbiaID'].tolist())
            # print(f"   📋 Using existing records info for match_log: {len(existing_ids)} existed")
        else:
            existing_ids = set()
            # print(f"   📋 No existing records provided - treating all match_log as new")
        
        # 2. 分離新增和更新
        new_match_log = match_log_df[~match_log_df['tbiaID'].isin(existing_ids)].copy()
        update_match_log = match_log_df[match_log_df['tbiaID'].isin(existing_ids)].copy()
        
        # print(f"   📝 New match_log: {len(new_match_log)}")
        # print(f"   🔄 Update match_log: {len(update_match_log)}")
        
        # 3. 批次新增
        if not new_match_log.empty:
            new_match_log.to_sql(
                'match_log',
                self.db,
                if_exists='append',
                index=False,
                chunksize=self.batch_size,
                method='multi'
            )
        
        # 4. 批次更新
        if not update_match_log.empty:
            self._batch_update_match_log(update_match_log)
        
        total_time = time.time() - start_time
        rate = len(match_log_df) / total_time if total_time > 0 else 0
        # print(f"✅ Match_log processing completed: {rate:.0f} records/sec")
    
    def _batch_update_match_log(self, update_df):
        """match_log 批次更新，使用固定的欄位類型（相對單純）"""
        if update_df.empty:
            return
            
        # 更新所有欄位（除了主鍵）
        exclude_cols = ['created', 'tbiaID']
        update_cols = [col for col in update_df.columns if col not in exclude_cols]
        
        if not update_cols:
            return
        
        # print(f"   🎯 批次更新 match_log {len(update_df)} 筆記錄...")
        
        # match_log 的固定欄位類型（相對單純）
        timestamp_cols = ['modified', 'created']
        numeric_cols = ['match_stage', 'stage_1', 'stage_2', 'stage_3', 'stage_4', 
                       'stage_5', 'stage_6', 'stage_7', 'stage_8']
        boolean_cols = ['match_higher_taxon', 'is_matched']
        
        # 使用大批次處理
        large_batch_size = min(1000, len(update_df))
        
        for i in range(0, len(update_df), large_batch_size):
            batch = update_df.iloc[i:i+large_batch_size]
            
            # 建立 VALUES 子句
            values_list = []
            for _, row in batch.iterrows():
                values = [f"'{row['tbiaID']}'"]  # tbiaID 作為鍵值
                
                for col in update_cols:
                    value = row[col]
                    
                    if pd.isna(value) or value is None:
                        values.append('NULL')
                    elif col in timestamp_cols:
                        # 時間戳記類型
                        values.append(f"{self._safe_dollar_quote(value)}::timestamp")
                    elif col in numeric_cols:
                        # 數值類型
                        if isinstance(value, (int, float)) and not pd.isna(value):
                            values.append(str(value))
                        else:
                            values.append('NULL')
                    elif col in boolean_cols:
                        # 布林類型
                        if isinstance(value, bool):
                            values.append('TRUE' if value else 'FALSE')
                        elif str(value).lower() in ['true', '1', 'yes', 't']:
                            values.append('TRUE')
                        elif str(value).lower() in ['false', '0', 'no', 'f']:
                            values.append('FALSE')
                        else:
                            values.append('NULL')
                    else:
                        values.append(self._safe_dollar_quote(value))

                values_list.append(f"({', '.join(values)})")
            
            # 建立批次更新 SQL
            values_clause = ',\n    '.join(values_list)
            
            # 建立 SET 子句
            set_clauses = []
            for j, col in enumerate(update_cols, 1):
                if col in timestamp_cols:
                    set_clauses.append(f'"{col}" = v.col_{j}::timestamp')
                elif col in numeric_cols:
                    set_clauses.append(f'"{col}" = v.col_{j}::numeric')
                elif col in boolean_cols:
                    set_clauses.append(f'"{col}" = v.col_{j}::boolean')
                else:
                    set_clauses.append(f'"{col}" = v.col_{j}')
            
            # 建立欄位別名
            col_aliases = ['tbia_id'] + [f'col_{j}' for j in range(1, len(update_cols) + 1)]
            
            batch_sql = f"""
            UPDATE match_log 
            SET {', '.join(set_clauses)}
            FROM (VALUES 
                {values_clause}
            ) AS v({', '.join(col_aliases)})
            WHERE match_log."tbiaID" = v.tbia_id;
            """
            
            try:
                with self.db.connect() as conn:
                    result = conn.exec_driver_sql(batch_sql)
                    conn.commit()
                    # print(f"     ✅ match_log 批次 {i//large_batch_size + 1}: 更新了 {result.rowcount} 筆")
                    
            except Exception as e:
                print(f"     ❌ match_log 批次更新失敗: {e}")
                # 回退到逐筆更新
                self._fallback_single_match_log_updates(batch, update_cols)
    
    def _fallback_single_match_log_updates(self, batch_df, update_cols):
        """match_log 回退到逐筆更新"""
        print(f"     🔄 match_log 回退到逐筆更新 {len(batch_df)} 筆...")
        
        for _, row in batch_df.iterrows():
            try:
                set_clause = ', '.join([f'"{col}" = :{col}' for col in update_cols])
                params = {col: row[col] for col in update_cols}
                params['tbiaID'] = row['tbiaID']
                
                update_sql = f"""
                UPDATE match_log 
                SET {set_clause}
                WHERE "tbiaID" = :tbiaID
                """
                
                with self.db.connect() as conn:
                    conn.execute(text(update_sql), params)
                    conn.commit()
                    
            except Exception as e:
                print(f"     ❌ match_log 單筆更新失敗 {row['tbiaID']}: {e}")
                failed = row.to_dict()
                failed['_error'] = str(e)
                failed['_table'] = 'match_log'
                self.failed_match_logs.append(failed)

    def export_failed_records(self, filepath='failed_match_logs.csv'):
        if self.failed_match_logs:
            pd.DataFrame(self.failed_match_logs).to_csv(filepath, index=False)
            print(f"📄 已匯出 {len(self.failed_match_logs)} 筆失敗記錄到 {filepath}")
        else:
            print("✅ 沒有失敗記錄")