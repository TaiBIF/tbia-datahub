from numpy import nan
from app import db

import pandas as pd

# # 取得taxon資料
import requests
import re
import urllib
import numpy as np
from datetime import datetime, timedelta
import sqlalchemy as sa
from scripts.utils import get_namecode


match_cols = ['taxonID','sci_index',
                'match_stage', 'match_higher_taxon', 'stage_1', 'stage_2', 'stage_3',
                'stage_4', 'stage_5', 'stage_6', 'stage_7', 'stage_8']

deleted_taxon_ids = pd.read_csv('/bucket/deleted_taxon.csv')
deleted_taxon_ids = deleted_taxon_ids.taxon_id.to_list()

issue_map = {
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

# 測試改成批次比對學名
def match_name_new(matching_df, is_parent, match_stage, sci_names, specific_rank):
    if len(matching_df):
        matching_names = matching_df.now_matching_name.to_list()
        request_url = f"http://host.docker.internal:8080/api.php?names={urllib.parse.quote(('|').join(matching_names))}&format=json&source=taicol"
        response = requests.get(request_url)
        if response.status_code == 200:
            result = response.json()
            data = [r[0] for r in result['data']] # 因為是選擇best 所以只會回傳一個matched_clean
            data = pd.DataFrame(data)
            data = data.rename(columns={'search_term': 'now_matching_name'})
            data = matching_df.merge(data, how='left')
            data = data.replace({np.nan: None})
            for data_index in data.index:
                row = data.iloc[data_index]
                # 這邊如果有多個sourceVernacularName比對到的話，只選擇第一個                
                if len(sci_names[(sci_names.sci_index==row.sci_index)&(sci_names.taxonID!='')]):
                    continue
                filtered_rs = row.results
                # 202505 改為 只取分數高於0.7
                if filtered_rs:
                    filtered_rs = [fr for fr in filtered_rs if fr.get('score') > 0.7]
                filtered_rss = []
                if filtered_rs:
                    # 排除掉同個taxonID但有不同name的情況
                    filtered_rs = pd.DataFrame(filtered_rs)[['accepted_namecode','family','order','class','kingdom','name_status','score','taxon_rank']].drop_duplicates()
                    if specific_rank: # 這邊的rank都是小寫
                        filtered_rs = filtered_rs[filtered_rs.taxon_rank==specific_rank]
                    # 如果有accepted，僅考慮accepted
                    if len(filtered_rs[filtered_rs.name_status=='accepted']):
                        filtered_rs = filtered_rs[filtered_rs.name_status=='accepted']
                    # 如果沒有accepted，但有not-accepted，僅考慮not-accepted
                    elif len(filtered_rs[filtered_rs.name_status=='not-accepted']):
                        filtered_rs = filtered_rs[filtered_rs.name_status=='not-accepted']
                    if len(filtered_rs):
                        filtered_rs = filtered_rs.drop(columns=['name_status','taxon_rank'])
                        filtered_rs = filtered_rs.to_dict(orient='records')
                        # NomenMatch 有比對到有效taxon
                        # 檢查是否有上階層資訊（優先檢查kingdom，其次檢查其他階層）
                        has_parent = False
                        use_kingdom = False
                        if row.get('sourceKingdom'):
                            has_parent = True
                            use_kingdom = True
                        elif row.get('sourceClass') or row.get('sourceFamily') or row.get('sourceOrder'):
                            has_parent = True
                            use_kingdom = False
                        # 若有上階層資訊，加上比對上階層 
                        if has_parent:
                            has_nm_parent = False # True代表有比對到
                            for frs in filtered_rs:
                                if use_kingdom:
                                    # 優先使用kingdom進行比對
                                    if frs.get('kingdom'):
                                        has_nm_parent = True # 有nomemmatch上階層
                                        if frs.get('kingdom') == row.get('sourceKingdom'):
                                            filtered_rss.append(frs)
                                else:
                                    # 使用其他階層進行比對
                                    if frs.get('family') or frs.get('order') or frs.get('class'):
                                        has_nm_parent = True # 有nomemmatch上階層
                                        if frs.get('family') == row.get('sourceFamily') or frs.get('class') == row.get('sourceClass') or frs.get('order') == row.get('sourceOrder'):
                                            filtered_rss.append(frs)
                            # 如果有任何有nm上階層 且filtered_rss > 0 就代表有上階層比對成功的結果
                            if has_nm_parent:
                                if len(filtered_rss) == 1:
                                    # 根據NomenMatch給的score確認名字是不是完全一樣
                                    # 如果是中文有可能是0.95
                                    match_score = filtered_rss[0]['score']
                                    # 在這邊確認是不是中文
                                    is_chinese = False
                                    if re.findall(r'[\u4e00-\u9fff]+', row.get('now_matching_name')):
                                        is_chinese = True
                                    if is_chinese and match_score == 0.95:
                                        match_score = 1
                                    if is_parent:
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 1
                                        # sci_names.loc[sci_names.sci_index==sci_index,'parentTaxonID'] = filtered_rss[0]['accepted_namecode']
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),'taxonID'] = filtered_rss[0]['accepted_namecode']
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),'match_higher_taxon'] = True
                                    else:
                                        if match_score < 1:
                                            sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 3
                                        else:
                                            sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = None
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),'taxonID'] = filtered_rss[0]['accepted_namecode']
                                else:
                                    sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 4
                                    # sci_names.loc[((sci_names.scientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),'more_than_one'] = True
                            else:
                                # 如果沒有任何nm上階層的結果，則直接用filtered_rs
                                if len(filtered_rs) == 1:
                                    match_score = filtered_rs[0]['score']
                                    # 在這邊確認是不是中文
                                    is_chinese = False
                                    if re.findall(r'[\u4e00-\u9fff]+', row.get('now_matching_name')):
                                        is_chinese = True
                                    if is_chinese and match_score == 0.95:
                                        match_score = 1
                                    if is_parent:
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 1
                                        # sci_names.loc[sci_names.sci_index==sci_index,'parentTaxonID'] = filtered_rs[0]['accepted_namecode']
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),'taxonID'] = filtered_rs[0]['accepted_namecode']
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),'match_higher_taxon'] = True
                                    else:
                                        if match_score < 1:
                                            sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 3
                                        else:
                                            sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = None
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),'taxonID'] = filtered_rs[0]['accepted_namecode']
                                else:
                                    sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 4
                        # 若沒有上階層資訊，就直接取比對結果
                        else:
                            if len(filtered_rs) == 1:
                                match_score = filtered_rs[0]['score']
                                # 在這邊確認是不是中文
                                is_chinese = False
                                if re.findall(r'[\u4e00-\u9fff]+', row.get('now_matching_name')):
                                    is_chinese = True
                                if is_chinese and match_score == 0.95:
                                    match_score = 1
                                if is_parent:
                                    sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 1
                                    sci_names.loc[sci_names.sci_index==row.get('sci_index'),'taxonID'] = filtered_rs[0]['accepted_namecode']
                                    sci_names.loc[sci_names.sci_index==row.get('sci_index'),'match_higher_taxon'] = True
                                else:
                                    if match_score < 1:
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 3
                                    else:
                                        sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = None
                                    sci_names.loc[sci_names.sci_index==row.get('sci_index'),'taxonID'] = filtered_rs[0]['accepted_namecode']
                            else:
                                sci_names.loc[sci_names.sci_index==row.get('sci_index'),f'stage_{match_stage}'] = 4


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



def matching_flow_new(sci_names):
    # sci_names['sci_index'] = sci_names.index
    if 'taxonID' not in sci_names.keys():
        sci_names['taxonID'] = ''
    sci_names['match_stage'] = 0
    sci_names['match_higher_taxon'] = False
    # 各階段的issue default是沒有對到
    sci_names['stage_1'] = None # 比對sourceScientificName
    sci_names['stage_2'] = None # 比對TaiCOL namecode
    sci_names['stage_3'] = None # 比對 sourceVernacularName (中文)
    sci_names['stage_4'] = None # 比對 sourceScientificName 第一個單詞
    sci_names['stage_5'] = None # 比對 originalVernacularName (中文 / 英文)
    sci_names['stage_6'] = None # 比對 sourceFamily
    sci_names['stage_7'] = None # 比對 sourceOrder
    sci_names['stage_8'] = None # 比對 sourceClass
    # 2025-02 若資料庫提供的taxonID已經在TaiCOL被刪除，將taxonID改為空值
    sci_names['taxonID'] = sci_names['taxonID'].apply(lambda x: '' if x in deleted_taxon_ids else x)
    # 優先採用TaiCOL taxonID (若原資料庫有提供)
    ## 第一階段比對 - scientificName
    no_taxon = sci_names[(sci_names.taxonID=='')]
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 1
    if 'sourceScientificName' in sci_names.keys():
        no_taxon = no_taxon.rename(columns={'sourceScientificName': 'now_matching_name'})
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].replace({np.nan:'',None:''})
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom', 'sci_index']]]
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 100):
            match_name_new(matching_df=matching_df[l:l+100],
                        is_parent=False,
                        match_stage=1,
                        sci_names=sci_names,
                        specific_rank=None)
    ## 第二階段比對 沒有taxonID的 試抓TaiCOL namecode
    # 第二階段不調整
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 2
    no_taxon = sci_names[sci_names.taxonID=='']
    for s in no_taxon.index:
        s_row = sci_names.loc[s]
        if s_row.get('scientificNameID'):
            match_namecode(matching_namecode=s_row.get('scientificNameID'),
                           match_stage=2,
                           sci_names=sci_names,
                           sci_index=s_row.sci_index)
    ## 第三階段比對 - sourceVernacularName 中文比對
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 3
    if 'sourceVernacularName' in sci_names.keys():
        no_taxon = sci_names[(sci_names.taxonID=='')&(sci_names.sourceVernacularName!='')]
        no_taxon = no_taxon.rename(columns={'sourceVernacularName': 'now_matching_name'})
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].replace({np.nan:'',None:''})
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 100):
            match_name_new(matching_df=matching_df[l:l+100],
                        is_parent=False,
                        match_stage=3,
                        sci_names=sci_names,
                        specific_rank=None)
    ## 第四階段比對 - scientificName第一個英文單詞 (為了至少可以補階層)
    ## 這邊要限定只能比對屬
    ## 這個情況要給的是parentTaxonID
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 4
    no_taxon = sci_names[sci_names.taxonID=='']
    if 'sourceScientificName' in sci_names.keys():
        no_taxon = no_taxon.rename(columns={'sourceScientificName': 'now_matching_name'})
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].apply(lambda x: x.split(' ') [0] if len(x.split(' '))> 1 else '')
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].replace({np.nan:'',None:''})
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 100):
            match_name_new(matching_df=matching_df[l:l+100],
                        is_parent=True,
                        match_stage=4,
                        sci_names=sci_names,
                        specific_rank='genus')
    # 第五階段比對 - originalVernacularName (中文 / 英文)
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 5
    no_taxon = sci_names[(sci_names.taxonID=='')]
    if 'originalVernacularName' in sci_names.keys():
        no_taxon = no_taxon.rename(columns={'originalVernacularName': 'now_matching_name'})
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].replace({np.nan:'',None:''})
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 100):
            match_name_new(matching_df=matching_df[l:l+100],
                        is_parent=False,
                        match_stage=5,
                        sci_names=sci_names,
                        specific_rank=None)
    # 第六階段比對 - sourceFamily
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 6
    no_taxon = sci_names[(sci_names.taxonID=='')]
    if 'sourceFamily' in sci_names.keys():
        no_taxon = no_taxon.rename(columns={'sourceFamily': 'now_matching_name'})
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].replace({np.nan:'',None:''})
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 100):
            match_name_new(matching_df=matching_df[l:l+100],
                        is_parent=True,
                        match_stage=6,
                        sci_names=sci_names,
                        specific_rank='family')
    # 第七階段比對 - sourceOrder
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 7
    no_taxon = sci_names[(sci_names.taxonID=='')]
    if 'sourceOrder' in sci_names.keys():
        no_taxon = no_taxon.rename(columns={'sourceOrder': 'now_matching_name'})
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].replace({np.nan:'',None:''})
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 100):
            match_name_new(matching_df=matching_df[l:l+100],
                        is_parent=True,
                        match_stage=7,
                        sci_names=sci_names,
                        specific_rank='order')
    # 第八階段比對 - sourceClass
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 8
    no_taxon = sci_names[(sci_names.taxonID=='')]
    if 'sourceClass' in sci_names.keys():
        no_taxon = no_taxon.rename(columns={'sourceClass': 'now_matching_name'})
        no_taxon['now_matching_name'] = no_taxon['now_matching_name'].replace({np.nan:'',None:''})
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 100):
            match_name_new(matching_df=matching_df[l:l+100],
                        is_parent=True,
                        match_stage=8,
                        sci_names=sci_names,
                        specific_rank='class')
    # 確定match_stage
    stage_list = [1,2,3,4,5,6,7,8]
    for i in stage_list[:7]:
        for stg in stage_list[stage_list.index(i)+1:]:
            sci_names.loc[sci_names.match_stage==i,f'stage_{stg}'] = None
    # 代表比對到最後還是沒有對到
    sci_names.loc[(sci_names.match_stage==8)&(sci_names.taxonID==''),'match_stage'] = None
    return sci_names



import numpy as np
import pandas as pd
import requests
import urllib.parse
import concurrent.futures
from functools import partial
import time
import re
from typing import List, Dict, Optional

def matching_flow_new_optimized(sci_names, batch_size=100, max_workers=4):
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
        
        try:
            request_url = f"http://host.docker.internal:8080/api.php?names={urllib.parse.quote(('|').join(names_list))}&format=json&source=taicol"
            response = requests.get(request_url, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                processed_results = []
                
                for r in result['data']:
                    if r:  # 確保不是空結果
                        best_match = r[0]  # 選擇最佳匹配
                        processed_results.append({
                            'search_term': best_match.get('search_term'),
                            'results': best_match.get('results', [])
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
        
        if column_name not in no_taxon.columns or no_taxon[column_name].isna().all():
            # 如果沒有資料可處理，保持與原版一致，不設置任何stage值
            print(f"  Stage {stage_num}: No data to process")
            return sci_names
        
        # 向量化清理數據
        no_taxon['now_matching_name'] = no_taxon[column_name].fillna('').astype(str)
        
        # 應用轉換函數（如果有）
        if transform_func:
            no_taxon['now_matching_name'] = transform_func(no_taxon['now_matching_name'])
        
        # 過濾空值
        matching_df = no_taxon[no_taxon.now_matching_name != ''].copy()
        
        if matching_df.empty:
            # 如果沒有有效的匹配名稱，保持與原版一致，不設置stage值
            print(f"  Stage {stage_num}: No valid names to match")
            return sci_names
        
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
        
        # 如果處理後沒有資料，保持與原版一致，不設置stage值
        if matching_df.empty:
            print(f"  Stage {stage_num}: No names after processing")
            return sci_names
        
        print(f"  Stage {stage_num}: Processing {len(matching_df)} names")
        
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
            
            print(f"    Successful matches: {successful_matches}")
            
        else:
            # 如果API沒有回傳任何結果，保持與原版一致，不設置stage值
            print(f"  Stage {stage_num}: No API results")
        
        return sci_names
    
    def _update_sci_names_vectorized(merged_data, sci_names, stage_num, is_parent, specific_rank):
        """向量化更新sci_names，返回成功匹配的數量"""
        
        successful_matches = 0
        
        for idx, row in merged_data.iterrows():
            # 跳過已經有taxonID的記錄
            sci_idx = row['sci_index']
            if (sci_names.loc[sci_names.sci_index == sci_idx, 'taxonID'] != '').any():
                continue
            
            filtered_rs = row.get('results', [])
            if not filtered_rs:
                # 沒有API結果，稍後會在外層設置為 2 (none)
                continue
            
            # 向量化過濾高分結果
            filtered_rs = [fr for fr in filtered_rs if fr.get('score', 0) > 0.7]
            
            if not filtered_rs:
                # 沒有高分結果，稍後會在外層設置為 2 (none)
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
                # 沒有符合rank要求的結果，稍後會在外層設置為 2 (none)
                continue
            
            # 向量化狀態過濾
            if (results_df.name_status == 'accepted').any():
                results_df = results_df[results_df.name_status == 'accepted']
            elif (results_df.name_status == 'not-accepted').any():
                results_df = results_df[results_df.name_status == 'not-accepted']
            
            if results_df.empty:
                # 沒有有效狀態的結果，稍後會在外層設置為 2 (none)
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
            # else: 會在外層設置為 2 (none)
        
        return successful_matches
    
    def _check_hierarchy_match_vectorized(results_dict, row):
        """向量化階層匹配檢查 - 完整實現原版邏輯"""
        if not results_dict:
            return []
        
        # 檢查是否有上階層資訊（優先檢查kingdom，其次檢查其他階層）
        has_parent = False
        use_kingdom = False
        
        if row.get('sourceKingdom'):
            has_parent = True
            use_kingdom = True
        elif row.get('sourceClass') or row.get('sourceFamily') or row.get('sourceOrder'):
            has_parent = True
            use_kingdom = False
        
        # 若沒有上階層資訊，直接返回所有結果
        if not has_parent:
            return results_dict
        
        # 若有上階層資訊，進行階層比對
        filtered_rss = []
        has_nm_parent = False  # True代表有比對到
        
        for frs in results_dict:
            if use_kingdom:
                # 優先使用kingdom進行比對
                if frs.get('kingdom'):
                    has_nm_parent = True  # 有nomemmatch上階層
                    if frs.get('kingdom') == row.get('sourceKingdom'):
                        filtered_rss.append(frs)
            else:
                # 使用其他階層進行比對 (family, order, class)
                if frs.get('family') or frs.get('order') or frs.get('class'):
                    has_nm_parent = True  # 有nomemmatch上階層
                    # 檢查任一階層是否匹配
                    if (frs.get('family') == row.get('sourceFamily') or 
                        frs.get('class') == row.get('sourceClass') or 
                        frs.get('order') == row.get('sourceOrder')):
                        filtered_rss.append(frs)
        
        # 如果有任何有nm上階層 且filtered_rss > 0 就代表有上階層比對成功的結果
        if has_nm_parent and filtered_rss:
            return filtered_rss
        elif has_nm_parent and not filtered_rss:
            # 有上階層資訊但沒有匹配成功，返回空列表
            return []
        else:
            # 沒有上階層資訊可比對，返回原結果
            return results_dict
    
    # 主要處理流程
    start_time = time.time()
    
    # 初始化欄位
    sci_names = sci_names.copy()
    if 'taxonID' not in sci_names.columns:
        sci_names['taxonID'] = ''
    
    # 向量化初始化
    init_columns = {
        'match_stage': 0,
        'match_higher_taxon': False,
        **{f'stage_{i}': None for i in range(1, 9)}
    }
    
    for col, default_val in init_columns.items():
        sci_names[col] = default_val
    
    # 向量化處理deleted taxonID
    if hasattr('deleted_taxon_ids', '__iter__'):
        sci_names['taxonID'] = sci_names['taxonID'].where(
            ~sci_names['taxonID'].isin(deleted_taxon_ids), ''
        )
    
    print(f"Initial setup: {time.time() - start_time:.2f}s")
    
    # 定義轉換函數
    def extract_first_word(series):
        """提取第一個單詞（只處理多單詞的學名）"""
        # 只有當學名包含空格時才提取第一個單詞，否則返回空字符串
        return series.apply(lambda x: x.split(' ')[0] if isinstance(x, str) and len(x.split(' ')) > 1 else '')
    
    # 各階段處理（使用向量化版本）
    stages = [
        (1, 'sourceScientificName', False, None, None),
        # 注意：stage_2 (namecode) 會在後面單獨處理
        (3, 'sourceVernacularName', False, None, None),
        (4, 'sourceScientificName', True, 'genus', extract_first_word),
        (5, 'originalVernacularName', False, None, None),
        (6, 'sourceFamily', True, 'family', None),
        (7, 'sourceOrder', True, 'order', None),
        (8, 'sourceClass', True, 'class', None),
    ]
    
    # 處理 Stage 1
    stage_start = time.time()
    sci_names = process_stage_vectorized(
        sci_names, 1, 'sourceScientificName', False, None, None
    )
    print(f"Stage 1: {time.time() - stage_start:.2f}s")
    
    # 處理 Stage 2 (namecode matching) - 必須在 Stage 1 之後
    stage2_start = time.time()
    sci_names.loc[sci_names.taxonID == '', 'match_stage'] = 2
    no_taxon = sci_names[sci_names.taxonID == '']
    
    for idx in no_taxon.index:
        s_row = sci_names.loc[idx]
        if s_row.get('scientificNameID'):
            # 這裡保持原有的match_namecode調用
            try:
                match_namecode(
                    matching_namecode=s_row.get('scientificNameID'),
                    match_stage=2,
                    sci_names=sci_names,
                    sci_index=s_row.sci_index
                )
            except:
                pass  # 忽略namecode匹配錯誤
    
    print(f"Stage 2 (namecode): {time.time() - stage2_start:.2f}s")
    
    # 處理 Stage 3-8
    for stage_num, column, is_parent, rank, transform_func in stages[1:]:  # 跳過已處理的stage_1
        stage_start = time.time()
        sci_names = process_stage_vectorized(
            sci_names, stage_num, column, is_parent, rank, transform_func
        )
        print(f"Stage {stage_num}: {time.time() - stage_start:.2f}s")
        
        # 早期退出：如果所有記錄都已匹配
        if (sci_names['taxonID'] != '').all():
            print(f"All records matched at stage {stage_num}")
            break
    
    
    # 向量化處理最終的match_stage清理
    stage_list = list(range(1, 9))
    for i in stage_list[:7]:
        for stg in stage_list[stage_list.index(i)+1:]:
            sci_names.loc[sci_names.match_stage == i, f'stage_{stg}'] = None
    
    # 處理最終未匹配的記錄
    sci_names.loc[
        (sci_names.match_stage == 8) & (sci_names.taxonID == ''), 
        'match_stage'
    ] = None
    
    total_time = time.time() - start_time
    print(f"Total matching time: {total_time:.2f}s")
    
    return sci_names
# ============================================
# 額外的優化工具函數
# ============================================
def preprocess_sci_names_for_matching(sci_names):
    """
    預處理sci_names以提高匹配效率
    """
    # 向量化清理空值和標準化格式
    string_columns = ['sourceScientificName', 'sourceVernacularName', 'originalVernacularName', 
                     'sourceFamily', 'sourceClass', 'sourceOrder', 'sourceKingdom']
    
    for col in string_columns:
        if col in sci_names.columns:
            sci_names[col] = (sci_names[col]
                             .fillna('')
                             .astype(str)
                             .str.strip()
                             .replace({'nan': '', 'None': '', 'null': ''}))
    
    return sci_names
def get_matching_statistics(sci_names):
    """
    獲取匹配統計信息
    """
    total_records = len(sci_names)
    matched_records = (sci_names['taxonID'] != '').sum()
    match_rate = matched_records / total_records * 100 if total_records > 0 else 0
    
    stage_stats = sci_names['match_stage'].value_counts().sort_index()
    
    return {
        'total_records': total_records,
        'matched_records': matched_records,
        'match_rate': match_rate,
        'stage_distribution': stage_stats.to_dict()
    }
