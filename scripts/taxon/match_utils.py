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
# def match_name_new(matching_name, is_parent, match_stage, sci_names, source_family, source_class, source_order, sci_index, specific_rank):
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
                    filtered_rs = pd.DataFrame(filtered_rs)[['accepted_namecode','family','order','class','name_status','score','taxon_rank']].drop_duplicates()
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
                        # 是否有上階層資訊
                        has_parent = False
                        if row.get('source_class') or row.get('source_family') or row.get('source_order'):
                            has_parent = True
                        # 若有上階層資訊，加上比對上階層 
                        if has_parent:
                            has_nm_parent = False # True代表有比對到
                            for frs in filtered_rs:
                                if frs.get('family') or frs.get('order') or frs.get('class'):
                                    if frs.get('family') == row.get('source_family') or frs.get('class') == row.get('source_class') or frs.get('order') == row.get('source_order'):
                                        filtered_rss.append(frs)
                                        has_nm_parent = True                            # if t_rank in ['種','種下階層']: # 直接比對family
                            # 如果有任何有nm上階層 且filtered_rss > 0 就代表有上階層比對成功的結果
                            if has_nm_parent:
                                if len(filtered_rss) == 1:
                                    # 根據NomenMatch給的score確認名字是不是完全一樣
                                    # 如果是中文有可能是0.95
                                    match_score = filtered_rss[0]['score']
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
    sci_names['sci_index'] = sci_names.index
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
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','source_family', 'source_class', 'source_order', 'sci_index']]]
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 20):
            match_name_new(matching_df=matching_df[l:l+20],
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
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','source_family', 'source_class', 'source_order', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 20):
            match_name_new(matching_df=matching_df[l:l+20],
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
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','source_family', 'source_class', 'source_order', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 20):
            match_name_new(matching_df=matching_df[l:l+20],
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
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','source_family', 'source_class', 'source_order', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 20):
            match_name_new(matching_df=matching_df[l:l+20],
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
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','source_family', 'source_class', 'source_order', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 20):
            match_name_new(matching_df=matching_df[l:l+20],
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
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','source_family', 'source_class', 'source_order', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 20):
            match_name_new(matching_df=matching_df[l:l+20],
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
        matching_df = no_taxon[no_taxon.now_matching_name!=''][[k for k in no_taxon.keys() if k in ['now_matching_name','source_family', 'source_class', 'source_order', 'sci_index']]]
        matching_df = matching_df.assign(now_matching_name=matching_df['now_matching_name'].str.split(';')).explode('now_matching_name')
        matching_df = matching_df.reset_index(drop=True)
        for l in range(0, len(matching_df), 20):
            match_name_new(matching_df=matching_df[l:l+20],
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






# def match_name(matching_name, is_parent, match_stage, sci_names, source_family, source_class, source_order, sci_index, specific_rank):
#     if matching_name:
#         # 先確定是不是中文
#         is_chinese = False
#         if re.findall(r'[\u4e00-\u9fff]+', matching_name):
#             is_chinese = True
#         request_url = f"http://host.docker.internal:8080/api.php?names={urllib.parse.quote(matching_name)}&format=json&source=taicol"
#         response = requests.get(request_url)
#         if response.status_code == 200:
#             result = response.json()
#             if result['data'][0][0]: # 因為一次只比對到一個，所以只要取第一個search term
#                 # 排除地位為誤用的taxon，因為代表該名字不該指涉到此taxon
#                 # match_score = result['data'][0][0].get('score')
#                 # if match_score == 'N/A': #有對到但無法計算分數
#                 #     match_score = 0 
#                 filtered_rs = result['data'][0][0]['results'] # 不用排除誤用，但優先序為 有效 -> 無效 -> 誤用
#                 filtered_rss = []
#                 if len(filtered_rs):
#                     # 排除掉同個taxonID但有不同name的情況
#                     filtered_rs = pd.DataFrame(filtered_rs)[['accepted_namecode','family','order','class','name_status','score','taxon_rank']].drop_duplicates()
#                     if specific_rank: # 這邊的rank都是小寫
#                         filtered_rs = filtered_rs[filtered_rs.taxon_rank==specific_rank]
#                     # 如果有accepted，僅考慮accepted
#                     if len(filtered_rs[filtered_rs.name_status=='accepted']):
#                         filtered_rs = filtered_rs[filtered_rs.name_status=='accepted']
#                     # 如果沒有accepted，但有not-accepted，僅考慮not-accepted
#                     elif len(filtered_rs[filtered_rs.name_status=='not-accepted']):
#                         filtered_rs = filtered_rs[filtered_rs.name_status=='not-accepted']
#                     if len(filtered_rs):
#                         filtered_rs = filtered_rs.drop(columns=['name_status','taxon_rank'])
#                         filtered_rs = filtered_rs.to_dict(orient='records')
#                         # NomenMatch 有比對到有效taxon
#                         # 是否有上階層資訊
#                         has_parent = False
#                         if source_class or source_family or source_order:
#                             has_parent = True
#                         # 若有上階層資訊，加上比對上階層 
#                         if has_parent:
#                             has_nm_parent = False # True代表有比對到
#                             for frs in filtered_rs:
#                                 if frs.get('family') or frs.get('order') or frs.get('class'):
#                                     if frs.get('family') == source_family or frs.get('class') == source_class or frs.get('order') == source_order:
#                                         filtered_rss.append(frs)
#                                         has_nm_parent = True                            # if t_rank in ['種','種下階層']: # 直接比對family
#                             # 如果有任何有nm上階層 且filtered_rss > 0 就代表有上階層比對成功的結果
#                             if has_nm_parent:
#                                 if len(filtered_rss) == 1:
#                                     # 根據NomenMatch給的score確認名字是不是完全一樣
#                                     # 如果是中文有可能是0.95
#                                     match_score = filtered_rss[0]['score']
#                                     if is_chinese and match_score == 0.95:
#                                         match_score = 1
#                                     if is_parent:
#                                         sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 1
#                                         # sci_names.loc[sci_names.sci_index==sci_index,'parentTaxonID'] = filtered_rss[0]['accepted_namecode']
#                                         sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rss[0]['accepted_namecode']
#                                         sci_names.loc[sci_names.sci_index==sci_index,'match_higher_taxon'] = True
#                                     else:
#                                         if match_score < 1:
#                                             sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 3
#                                         else:
#                                             sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = None
#                                         sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rss[0]['accepted_namecode']
#                                 else:
#                                     sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 4
#                                     # sci_names.loc[((sci_names.scientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),'more_than_one'] = True
#                             else:
#                                 # 如果沒有任何nm上階層的結果，則直接用filtered_rs
#                                 if len(filtered_rs) == 1:
#                                     match_score = filtered_rs[0]['score']
#                                     if is_chinese and match_score == 0.95:
#                                         match_score = 1
#                                     if is_parent:
#                                         sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 1
#                                         # sci_names.loc[sci_names.sci_index==sci_index,'parentTaxonID'] = filtered_rs[0]['accepted_namecode']
#                                         sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rs[0]['accepted_namecode']
#                                         sci_names.loc[sci_names.sci_index==sci_index,'match_higher_taxon'] = True
#                                     else:
#                                         if match_score < 1:
#                                             sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 3
#                                         else:
#                                             sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = None
#                                         sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rs[0]['accepted_namecode']
#                                 else:
#                                     sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 4
#                         # 若沒有上階層資訊，就直接取比對結果
#                         else:
#                             if len(filtered_rs) == 1:
#                                 match_score = filtered_rs[0]['score']
#                                 if is_chinese and match_score == 0.95:
#                                     match_score = 1
#                                 if is_parent:
#                                     sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 1
#                                     sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rs[0]['accepted_namecode']
#                                     sci_names.loc[sci_names.sci_index==sci_index,'match_higher_taxon'] = True
#                                 else:
#                                     if match_score < 1:
#                                         sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 3
#                                     else:
#                                         sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = None
#                                     sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rs[0]['accepted_namecode']
#                             else:
#                                 sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 4



# def matching_flow(sci_names):
#     sci_names['sci_index'] = sci_names.index
#     if 'taxonID' not in sci_names.keys():
#         sci_names['taxonID'] = ''
#     sci_names['match_stage'] = 0
#     sci_names['match_higher_taxon'] = False
#     # 各階段的issue default是沒有對到
#     sci_names['stage_1'] = None # 比對sourceScientificName
#     sci_names['stage_2'] = None # 比對TaiCOL namecode
#     sci_names['stage_3'] = None # 比對 sourceVernacularName (中文)
#     sci_names['stage_4'] = None # 比對 sourceScientificName 第一個單詞
#     sci_names['stage_5'] = None # 比對 originalVernacularName (中文 / 英文)
#     sci_names['stage_6'] = None # 比對 sourceFamily
#     sci_names['stage_7'] = None # 比對 sourceOrder
#     sci_names['stage_8'] = None # 比對 sourceClass
#     # 優先採用TaiCOL taxonID (若原資料庫有提供)
#     ## 第一階段比對 - scientificName
#     no_taxon = sci_names[(sci_names.taxonID=='')]
#     sci_names.loc[sci_names.taxonID=='','match_stage'] = 1
#     for s in no_taxon.index:
#         s_row = sci_names.loc[s]
#         if s_row.sourceScientificName:
#             match_name(matching_name=s_row.sourceScientificName,
#                        is_parent=False,
#                        match_stage=1,
#                        sci_names=sci_names,
#                        source_family=s_row.get('sourceFamily'), 
#                        source_class=s_row.get('sourceClass'), 
#                        source_order=s_row.get('sourceOrder'),
#                        sci_index=s_row.sci_index,
#                        specific_rank=None)
#     ## 第二階段比對 沒有taxonID的 試抓TaiCOL namecode
#     sci_names.loc[sci_names.taxonID=='','match_stage'] = 2
#     no_taxon = sci_names[sci_names.taxonID=='']
#     for s in no_taxon.index:
#         s_row = sci_names.loc[s]
#         if s_row.get('scientificNameID'):
#             match_namecode(matching_namecode=s_row.get('scientificNameID'),
#                            match_stage=2,
#                            sci_names=sci_names,
#                            sci_index=s_row.sci_index)
#     ## 第三階段比對 - sourceVernacularName 中文比對
#     sci_names.loc[sci_names.taxonID=='','match_stage'] = 3
#     no_taxon = sci_names[(sci_names.taxonID=='')&(sci_names.sourceVernacularName!='')]
#     for nti in no_taxon.sci_index.unique():
#         # 可能有多個sourceVernacularName
#         s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
#         if names := s_row.get('sourceVernacularName'):
#             for nn in names.split(';'):
#                 if not sci_names.loc[nti,'taxonID']:
#                     match_name(matching_name=nn,
#                                is_parent=False,
#                                match_stage=3,
#                                sci_names=sci_names,
#                                source_family=s_row.get('sourceFamily'), 
#                                source_class=s_row.get('sourceClass'), 
#                                source_order=s_row.get('sourceOrder'),
#                                sci_index=s_row.get('sci_index'),
#                                specific_rank=None)
#     ## 第四階段比對 - scientificName第一個英文單詞 (為了至少可以補階層)
#     ## 這邊要限定只能比對屬
#     ## 這個情況要給的是parentTaxonID
#     sci_names.loc[sci_names.taxonID=='','match_stage'] = 4
#     no_taxon = sci_names[sci_names.taxonID=='']
#     for nti in no_taxon.sci_index.unique():
#         s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
#         if nt_str := s_row.get('sourceScientificName'):
#             if len(nt_str.split(' ')) > 1: # 等於0的話代表上面已經對過了
#                 match_name(matching_name=nt_str.split(' ')[0], 
#                            is_parent=True,
#                            match_stage=4,
#                            sci_names=sci_names,
#                            source_family=s_row.get('sourceFamily'), 
#                            source_class=s_row.get('sourceClass'), 
#                            source_order=s_row.get('sourceOrder'),
#                            sci_index=s_row.get('sci_index'),
#                            specific_rank='genus')
#     # 第五階段比對 - originalVernacularName (中文 / 英文)
#     sci_names.loc[sci_names.taxonID=='','match_stage'] = 5
#     no_taxon = sci_names[(sci_names.taxonID=='')]
#     for nti in no_taxon.sci_index.unique():
#         s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
#         if s_row.get('originalVernacularName'):
#             match_name(matching_name=s_row.get('originalVernacularName'), 
#                         is_parent=False,
#                         match_stage=5,
#                         sci_names=sci_names,
#                         source_family=s_row.get('sourceFamily'), 
#                         source_class=s_row.get('sourceClass'), 
#                         source_order=s_row.get('sourceOrder'),
#                         sci_index=s_row.get('sci_index'),
#                         specific_rank=None)
#     # 第六階段比對 - sourceFamily
#     sci_names.loc[sci_names.taxonID=='','match_stage'] = 6
#     no_taxon = sci_names[(sci_names.taxonID=='')]
#     for nti in no_taxon.sci_index.unique():
#         s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
#         if s_row.get('sourceFamily'):
#             match_name(matching_name=s_row.get('sourceFamily'), 
#                         is_parent=True,
#                         match_stage=6,
#                         sci_names=sci_names,
#                         source_family=s_row.get('sourceFamily'), 
#                         source_class=s_row.get('sourceClass'), 
#                         source_order=s_row.get('sourceOrder'),
#                         sci_index=s_row.get('sci_index'),
#                         specific_rank='family')
#     # 第七階段比對 - sourceOrder
#     sci_names.loc[sci_names.taxonID=='','match_stage'] = 7
#     no_taxon = sci_names[(sci_names.taxonID=='')]
#     for nti in no_taxon.sci_index.unique():
#         s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
#         if s_row.get('sourceOrder'):
#             match_name(matching_name=s_row.get('sourceOrder'), 
#                         is_parent=True,
#                         match_stage=7,
#                         sci_names=sci_names,
#                         source_family=s_row.get('sourceFamily'), 
#                         source_class=s_row.get('sourceClass'), 
#                         source_order=s_row.get('sourceOrder'),
#                         sci_index=s_row.get('sci_index'),
#                         specific_rank='order')
#     # 第八階段比對 - sourceClass
#     sci_names.loc[sci_names.taxonID=='','match_stage'] = 8
#     no_taxon = sci_names[(sci_names.taxonID=='')]
#     for nti in no_taxon.sci_index.unique():
#         s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
#         if s_row.get('sourceClass'):
#             match_name(matching_name=s_row.get('sourceClass'), 
#                         is_parent=True,
#                         match_stage=8,
#                         sci_names=sci_names,
#                         source_family=s_row.get('sourceFamily'), 
#                         source_class=s_row.get('sourceClass'), 
#                         source_order=s_row.get('sourceOrder'),
#                         sci_index=s_row.get('sci_index'),
#                         specific_rank='class')
#     # 確定match_stage
#     stage_list = [1,2,3,4,5,6,7,8]
#     for i in stage_list[:7]:
#         for stg in stage_list[stage_list.index(i)+1:]:
#             sci_names.loc[sci_names.match_stage==i,f'stage_{stg}'] = None
#     # 代表比對到最後還是沒有對到
#     sci_names.loc[(sci_names.match_stage==8)&(sci_names.taxonID==''),'match_stage'] = None
#     return sci_names
