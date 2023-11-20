from numpy import nan
from app import db

import pandas as pd

# 取得taxon資料
import requests
import re
import urllib
import numpy as np
from datetime import datetime, timedelta

import sqlalchemy as sa

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



# with db.begin() as conn:
#     qry = sa.text("select * from taxon")
#     resultset = conn.execute(qry)
#     taxon = resultset.mappings().all()

# taxon = pd.DataFrame(taxon)
# taxon = taxon.drop(columns=['scientificNameID','id'])


url = "http://solr:8983/solr/taxa/select?indent=true&q.op=OR&q=*%3A*&rows=2147483647"
resp = requests.get(url)
taxon = resp.json()['response']['docs']

taxon = pd.DataFrame(taxon)
taxon = taxon.rename(columns={'id': 'taxonID'})
taxon = taxon.drop(columns=['taxon_name_id','_version_'])
taxon = taxon.replace({nan:None})


def match_name(matching_name, sci_name, original_name, is_parent, match_stage, sci_names, source_family, source_class, source_order, sci_index):
    if matching_name:
        request_url = f"http://host.docker.internal:8080/api.php?names={urllib.parse.quote(matching_name)}&format=json&source=taicol"
        response = requests.get(request_url)
        if response.status_code == 200:
            result = response.json()
            if result['data'][0][0]: # 因為一次只比對到一個，所以只要取第一個search term
                # 排除地位為誤用的taxon，因為代表該名字不該指涉到此taxon
                match_score = result['data'][0][0].get('score')
                if match_score == 'N/A': #有對到但無法計算分數
                    match_score = 0 
                # filtered_rs = [rs for rs in result['data'][0][0]['results'] if rs['name_status'] != 'misapplied']
                filtered_rs = result['data'][0][0]['results'] # 不用排除誤用，但優先序為 有效 -> 無效 -> 誤用
                filtered_rss = []
                if len(filtered_rs):
                    # 排除掉同個taxonID但有不同name的情況
                    filtered_rs = pd.DataFrame(filtered_rs)[['accepted_namecode','family','order','class','name_status']].drop_duplicates()
                    # 如果有accepted，僅考慮accepted
                    if len(filtered_rs[filtered_rs.name_status=='accepted']):
                        filtered_rs = filtered_rs[filtered_rs.name_status=='accepted']
                    # 如果沒有accepted，但有not-accepted，僅考慮not-accepted
                    elif len(filtered_rs[filtered_rs.name_status=='not-accepted']):
                        filtered_rs = filtered_rs[filtered_rs.name_status=='not-accepted']
                    filtered_rs = filtered_rs.drop(columns=['name_status'])
                    filtered_rs = filtered_rs.to_dict(orient='records')
                    # NomenMatch 有比對到有效taxon
                    # sci_names.loc[sci_names.sci_index==sci_index,'has_nm_result'] = True
                    # 是否有上階層資訊
                    has_parent = False
                    if source_family or source_class or source_order:
                        has_parent = True
                    # if original_taxonuuid:
                    #     tbn_url = "https://www.tbn.org.tw/api/v25/taxon?uuid=" + original_taxonuuid
                    #     tbn_response = requests.get(tbn_url)
                    #     if tbn_response.status_code == 200:
                    #         if tbn_data := tbn_response.json().get('data'):
                    #             t_family = tbn_data[0].get('family') # 科
                    #             t_class = tbn_data[0].get('class') # 綱
                    #             t_rank = tbn_data[0].get('taxonRank')
                    #             t_patent_uuid = tbn_data[0].get('parentUUID')
                    # 若有上階層資訊，加上比對上階層                    
                    if has_parent:
                        has_nm_parent = False
                        for frs in filtered_rs:
                            # if t_rank in ['種','種下階層']: # 直接比對family
                            #     if frs.get('family'):
                            #         if frs.get('family') == t_family:
                            #             filtered_rss.append(frs)
                            #             has_nm_parent = True
                            #             # 本來就沒有上階層的話就不管
                            # elif t_rank in ['亞綱','總目','目','亞目','總科','科','亞科','屬','亞屬']: # 
                            #     if frs.get('family') or frs.get('class'):
                            if frs.get('family') == source_family or frs.get('class') == source_class or frs.get('order') == source_order:
                                filtered_rss.append(frs)
                                has_nm_parent = True
                            else:
                                has_nm_parent = False # TODO 這邊先當成沒有nm上階層，直接比對學名
                            # elif t_rank in ['綱','總綱','亞門']: # 還要再往上抓到門
                            # elif t_rank in ['亞界','總門','門']: #  還要再往上抓到界
                        # 如果有任何有nm上階層 且filtered_rss > 0 就代表有上階層比對成功的結果
                        if has_nm_parent:
                            if len(filtered_rss) == 1:
                                if is_parent:
                                    sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 1
                                    # sci_names.loc[sci_names.sci_index==sci_index,'parentTaxonID'] = filtered_rss[0]['accepted_namecode']
                                    sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rss[0]['accepted_namecode']
                                    sci_names.loc[sci_names.sci_index==sci_index,'match_higher_taxon'] = True
                                else:
                                    # 根據NomenMatch給的score確認名字是不是完全一樣
                                    if match_score < 1:
                                        sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 3
                                    else:
                                        sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = None
                                    sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rss[0]['accepted_namecode']
                            else:
                                sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 4
                        else:
                            # 如果沒有任何nm上階層的結果，則直接用filtered_rs
                            if len(filtered_rs) == 1:
                                if is_parent:
                                    sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 1
                                    # sci_names.loc[sci_names.sci_index==sci_index,'parentTaxonID'] = filtered_rs[0]['accepted_namecode']
                                    sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rs[0]['accepted_namecode']
                                    sci_names.loc[sci_names.sci_index==sci_index,'match_higher_taxon'] = True
                                else:
                                    if match_score < 1:
                                        sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 3
                                    else:
                                        sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = None
                                    sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rs[0]['accepted_namecode']
                            else:
                                sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 4
                                # sci_names.loc[sci_names.sci_index==sci_index,'more_than_one'] = True
                    # 若沒有上階層資訊，就直接取比對結果
                    else:
                        if len(filtered_rs) == 1:
                            if is_parent:
                                sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 1
                                # sci_names.loc[sci_names.sci_index==sci_index,'parentTaxonID'] = filtered_rs[0]['accepted_namecode']
                                sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rs[0]['accepted_namecode']
                                sci_names.loc[sci_names.sci_index==sci_index,'match_higher_taxon'] = True
                            else:
                                if match_score < 1:
                                    sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 3
                                else:
                                    sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = None
                                sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = filtered_rs[0]['accepted_namecode']
                        else:
                            sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 4
                # else:
                #     sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 2 # none


def match_namecode(matching_namecode, sci_name, match_stage, sci_names, sci_index):
    # 這邊不會有fuzzy的問題 因為直接用namecode對應
    # 也不考慮高階層
    try:
        matching_namecode = str(int(matching_namecode))
    except:
        pass
    # 改成用TaiCOL API
    # taxon_name_id = None
    taxon_data = []
    name_res = requests.get(f'https://api.taicol.tw/v2/namecode?namecode={matching_namecode}')
    if name_res.status_code == 200:
        if name_data := name_res.json().get('data'):
            # for d in name_data:
            #     print(d)
            # tmp = [d['taxon'] for d in name_data if d.get('usage_status') !='misapplied']
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
            # for n in name_data:
            #     for tt in n.get('taxon'):
            #         if tt.get('usage_status') != 'misapplied':
            #             taxon_data.append(tt.get('taxon_id'))
            if len(taxon_data) == 1:
                sci_names.loc[sci_names.sci_index==sci_index,'taxonID'] = taxon_data[0]
                sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = None
            else:
                sci_names.loc[sci_names.sci_index==sci_index,f'stage_{match_stage}'] = 4


def matching_flow(sci_names):
    sci_names['sci_index'] = sci_names.index
    sci_names['taxonID'] = ''
    sci_names['match_higher_taxon'] = False
    # sci_names['parentTaxonID'] = ''
    sci_names['match_stage'] = 0
    # 各階段的issue default是沒有對到
    sci_names['stage_1'] = None
    sci_names['stage_2'] = None
    sci_names['stage_3'] = None
    sci_names['stage_4'] = None
    sci_names['stage_5'] = None
    # 優先採用TaiCOL taxonID (若原資料庫有提供)
    ## 第一階段比對 - sourceScientificName
    no_taxon = sci_names[(sci_names.taxonID=='')]
    for s in sci_names.index:
        s_row = sci_names.loc[s]
        if s_row.sourceScientificName:
            # matching_name, sci_name, original_name, original_taxonuuid, is_parent, match_stage, sci_names, source_family, source_class, source_order, sci_index
            match_name(matching_name=s_row.sourceScientificName,
                       sci_name=s_row.sourceScientificName,
                       original_name=s_row.originalScientificName,
                    #    original_taxonuuid=s_row.sourceTaxonID,
                       is_parent=False,
                       match_stage=1,
                       sci_names=sci_names,
                       source_family=s_row.get('sourceFamily'), 
                       source_class=s_row.get('sourceClass'), 
                       source_order=s_row.get('sourceOrder'),
                       sci_index=s_row.sci_index)
    ## 第二階段比對 沒有taxonID的 試抓TaiCOL namecode
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 2
    no_taxon = sci_names[sci_names.taxonID=='']
    for s in no_taxon.index:
        s_row = sci_names.loc[s]
        if s_row.scientificNameID:
            match_namecode(matching_namecode=s_row.get('scientificNameID'),
                sci_name=s_row.sourceScientificName,
                match_stage=2,
                sci_names=sci_names,
                sci_index=s_row.sci_index)
    ## 第三階段比對 - originalScientificName 英文比對
    ## 第三階段比對 - originalScientificName 中文比對
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 3
    no_taxon = sci_names[(sci_names.taxonID=='')&(sci_names.originalScientificName!='')]
    # 要判斷是中文還是英文(英文可能帶有標點符號)
    for nti in no_taxon.sci_index.unique():
        s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
        nt_str = s_row.get('originalScientificName')
        # 拿掉階層名
        for v in rank_map.values():
            nt_str = nt_str.replace(v, '')
        # 拿掉空格
        nt_str = re.sub(' +', ' ', nt_str)
        str_list = nt_str.split(' ')
        # 英文的部分組合起來
        eng_part = [' '.join([s for s in str_list if not any(re.findall(r'[\u4e00-\u9fff]+', s))])]
        c_part = [s for s in str_list if re.findall(r'[\u4e00-\u9fff]+', s)]
        str_list = eng_part + c_part
        for sl in str_list:
            if sl:
                if not any(re.findall(r'[\u4e00-\u9fff]+', sl)):
                    # 英文
                    match_name(matching_name=sl, 
                               sci_name=s_row.get('sourceScientificName'),
                               original_name=s_row.get('originalScientificName'),
                            #    original_taxonuuid=sci_names.loc[nti,'sourceTaxonID'],
                               is_parent=False,
                               match_stage=3,
                               sci_names=sci_names,
                               source_family=s_row.get('sourceFamily'), 
                               source_class=s_row.get('sourceClass'), 
                               source_order=s_row.get('sourceOrder'),
                               sci_index=s_row.get('sci_index'))
                    # 如果對到就break
                    if sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0].get('taxonID'):
                        break
                else:
                    # 中文
                    match_name(matching_name=sl, 
                               sci_name=s_row.get('sourceScientificName'),
                               original_name=s_row.get('originalScientificName'),
                            #    original_taxonuuid=sci_names.loc[nti,'sourceTaxonID'],
                               is_parent=False,
                               match_stage=3,
                               sci_names=sci_names,
                               source_family=s_row.get('sourceFamily'), 
                               source_class=s_row.get('sourceClass'), 
                               source_order=s_row.get('sourceOrder'),
                               sci_index=s_row.get('sci_index'))
    ## 第四階段比對 - scientificName第一個英文單詞 (為了至少可以補階層)
    ## 這個情況要給的是parentTaxonID
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 4
    no_taxon = sci_names[sci_names.taxonID=='']
    for nti in no_taxon.sci_index.unique():
        s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
        if nt_str := s_row.get('sourceScientificName'):
            if len(nt_str.split(' ')) > 1: # 等於0的話代表上面已經對過了
                match_name(matching_name=nt_str.split(' ')[0], 
                           sci_name=s_row.get('sourceScientificName'),
                           original_name=s_row.get('originalScientificName'),
                        #    original_taxonuuid=sci_names.loc[nti,'sourceTaxonID'],
                           is_parent=True,
                           match_stage=4,
                           sci_names=sci_names,
                           source_family=s_row.get('sourceFamily'), 
                           source_class=s_row.get('sourceClass'), 
                           source_order=s_row.get('sourceOrder'),
                           sci_index=s_row.get('sci_index'))
    ## 第五階段比對 - originalScientificName第一個英文單詞 (為了至少可以補階層)
    ## 這個情況要給的是parentTaxonID
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 5
    # no_taxon = sci_names[(sci_names.taxonID=='')&(sci_names.parentTaxonID=='')]
    no_taxon = sci_names[(sci_names.taxonID=='')]
    for nti in no_taxon.sci_index.unique():
        s_row = sci_names.loc[sci_names.sci_index==nti].to_dict('records')[0]
        if nt_str := s_row.get('originalScientificName'):
            if len(nt_str.split(' ')) > 1: # 等於0的話代表上面已經對過了
                # 以TBN的資料來說應該第一個是英文 但再確認一次
                # nt_str = sci_names.loc[nti,'originalScientificName']
                # 拿掉階層名
                for v in rank_map.values():
                    nt_str = nt_str.replace(v, '')
                # 拿掉空格
                nt_str = re.sub(' +', ' ', nt_str)
                str_list = nt_str.split(' ')
                # 英文的部分組合起來
                eng_part = ' '.join([s for s in str_list if not any(re.findall(r'[\u4e00-\u9fff]+', s))])
                # if not any(re.findall(r'[\u4e00-\u9fff]+', eng_part)):
                match_name(matching_name=eng_part.split(' ')[0], 
                           sci_name=s_row.get('sourceScientificName'),
                           original_name=s_row.get('originalScientificName'),
                        #    original_taxonuuid=sci_names.loc[nti,'sourceTaxonID'],
                           is_parent=True,
                           match_stage=5,
                           sci_names=sci_names,
                           source_family=s_row.get('sourceFamily'), 
                           source_class=s_row.get('sourceClass'), 
                           source_order=s_row.get('sourceOrder'),
                           sci_index=s_row.get('sci_index'))
    # 確定match_stage
    stage_list = [1,2,3,4,5]
    for i in stage_list[:4]:
        for stg in stage_list[stage_list.index(i)+1:]:
            sci_names.loc[sci_names.match_stage==i,f'stage_{stg}'] = None
    sci_names.loc[(sci_names.match_stage==5)&(sci_names.taxonID==''),'match_stage'] = None
    # sci_names.loc[(sci_names.match_stage==5)&(sci_names.taxonID=='')&(sci_names.parentTaxonID==''),'match_stage'] = None
    return sci_names

