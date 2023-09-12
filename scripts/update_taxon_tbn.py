# 用solr先取回更新的數量
# curl "http://127.0.0.1:8983/solr/tbia_records/select?facet.field=group&facet=true&indent=true&rows=0&q.op=OR&q=*%3A*"


# TBN流程
# "tbri",859340,
# "gbif",13922807,
# "oca",15952 # OCA的還沒抓回taxonUUID

# TaiBIF流程
# "brcas",837212,
# "ntm",23097,
# "wra",143694,


# 其他流程
# "forest",1526663,
# "cpami",781492,
# "taif",385669,
# "tcd",208144,
# "fact",175183,
# "brmas",126480,

# from scripts.taxon_match_utils import *
from numpy import nan
from app import db

# 2023-09-04 重新比對學名
import pandas as pd

from app import portal_db_settings
# 取得taxon資料
import psycopg2
import requests
import re
import urllib
import numpy as np
from datetime import datetime, timedelta


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



conn = psycopg2.connect(**portal_db_settings)

cur = conn.cursor()
cur.execute('SELECT * FROM data_taxon')
taxon = cur.fetchall()
colnames = [desc[0] for desc in cur.description]
taxon = pd.DataFrame(taxon, columns=colnames)
sub_col = [c for c in colnames if c not in ['id', 'iucn', 'redlist', 'protected', 'sensitive', 'cites']]
taxon = taxon[sub_col]


# sci_names = pd.DataFrame()

def match_name_tbn(matching_name,sci_name,original_name,original_taxonuuid,is_parent,match_stage):
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
                filtered_rs = [rs for rs in result['data'][0][0]['results'] if rs['name_status'] != 'misapplied']
                filtered_rss = []
                if len(filtered_rs):
                    # 排除掉同個taxonID但有不同name的情況
                    filtered_rs = pd.DataFrame(filtered_rs)[['accepted_namecode','family','class']].drop_duplicates().to_dict(orient='records')
                    # NomenMatch 有比對到有效taxon
                    # sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'has_nm_result'] = True
                    # 是否有上階層資訊
                    has_parent = False
                    if original_taxonuuid:
                        tbn_url = "https://www.tbn.org.tw/api/v25/taxon?uuid=" + original_taxonuuid
                        tbn_response = requests.get(tbn_url)
                        if tbn_response.status_code == 200:
                            if tbn_data := tbn_response.json().get('data'):
                                t_family = tbn_data[0].get('family') # 科
                                t_class = tbn_data[0].get('class') # 綱
                                t_rank = tbn_data[0].get('taxonRank')
                                t_patent_uuid = tbn_data[0].get('parentUUID')
                                has_parent = True
                    # 若有上階層資訊，加上比對上階層                    
                    if has_parent:
                        has_nm_parent = False
                        for frs in filtered_rs:
                            if t_rank in ['種','種下階層']: # 直接比對family
                                if frs.get('family'):
                                    if frs.get('family') == t_family:
                                        filtered_rss.append(frs)
                                        has_nm_parent = True
                                        # 本來就沒有上階層的話就不管
                            elif t_rank in ['亞綱','總目','目','亞目','總科','科','亞科','屬','亞屬']: # 
                                if frs.get('family') or frs.get('class'):
                                    if frs.get('family') == t_family or frs.get('class') == t_class:
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
                                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 1
                                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'parentTaxonID'] = filtered_rss[0]['accepted_namecode']
                                else:
                                    # 根據NomenMatch給的score確認名字是不是完全一樣
                                    if match_score < 1:
                                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 3
                                    else:
                                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = None
                                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'taxonID'] = filtered_rss[0]['accepted_namecode']
                            else:
                                sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 4
                        else:
                            # 如果沒有任何nm上階層的結果，則直接用filtered_rs
                            if len(filtered_rs) == 1:
                                if is_parent:
                                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 1
                                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'parentTaxonID'] = filtered_rs[0]['accepted_namecode']
                                else:
                                    if match_score < 1:
                                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 3
                                    else:
                                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = None
                                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'taxonID'] = filtered_rs[0]['accepted_namecode']
                            else:
                                sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 4
                                # sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'more_than_one'] = True
                    # 若沒有上階層資訊，就直接取比對結果
                    else:
                        if len(filtered_rs) == 1:
                            if is_parent:
                                sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 1
                                sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'parentTaxonID'] = filtered_rs[0]['accepted_namecode']
                            else:
                                if match_score < 1:
                                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 3
                                else:
                                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = None
                                sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'taxonID'] = filtered_rs[0]['accepted_namecode']
                        else:
                            sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 4
                # else:
                #     sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 2 # none


def match_namecode_tbn(matching_namecode,sci_name,original_name,original_taxonuuid,match_stage):
    # 這邊不會有fuzzy的問題 因為直接用namecode對應
    try:
        matching_namecode = str(int(matching_namecode))
    except:
        pass
    # 改成用TaiCOL API
    taxon_name_id = None
    taxon_data = []
    name_res = requests.get(f'https://api.taicol.tw/v2/namecode?namecode={matching_namecode}')
    # TODO 這邊回傳的資料有修改
    if name_res.status_code == 200:
        if name_data := name_res.json().get('data'):
            taxon_data = [d['taxon'] for d in name_data if d.get('usage_status') !='misapplied']
            # if len(syns) == 1:
            #     taxon_name_id = name_data[0].get('name_id')
    # if taxon_name_id:
    #     taxon_res = requests.get(f'https://api.taicol.tw/v2/nameMatch?name_id={taxon_name_id}')
    #     if taxon_res.status_code == 200:
    #         if taxon_data := taxon_res.json().get('data'):
    #             # 排除誤用
    #             taxon_data = [t.get('taxon_id') for t in taxon_data if t.get('usage_status') != 'Misapplied']
        filtered_rs = []
        # 可能對到不只一個taxon
        # if Taxon.objects.filter(taxonID__in=taxon_data).exists():
        if len(taxon[taxon.taxonID.isin(taxon_data)]):
            has_parent = False
            # namecode有對應到有效Taxon
            # sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'has_namecode_result'] = True
            # matched_taxon = Taxon.objects.filter(taxonID__in=taxon_data).values()
            matched_taxon = taxon[taxon.taxonID.isin(taxon_data)].to_dict('records')
            if original_taxonuuid:
                tbn_url = "https://www.tbn.org.tw/api/v25/taxon?uuid=" + original_taxonuuid
                tbn_response = requests.get(tbn_url)
                if tbn_response.status_code == 200:
                    if tbn_data := tbn_response.json().get('data'):
                        t_family = tbn_data[0].get('family') # 科
                        t_class = tbn_data[0].get('class') # 綱
                        t_rank = tbn_data[0].get('taxonRank')
                        t_patent_uuid = tbn_data[0].get('parentUUID')
                        has_parent = True
                        # if t_family or t_class:
                        #     sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'has_tbn_parent'] = True
            # 若有上階層資訊，加上比對上階層                    
            if has_parent:
                has_taxon_parent = False
                for frs in matched_taxon:
                    if t_rank in ['種','種下階層']: # 直接比對family
                        if frs.get('family'):
                            if frs.get('family') == t_family:
                                filtered_rs.append(frs)
                                has_taxon_parent = True
                                # 本來就沒有上階層的話就不管
                    elif t_rank in ['亞綱','總目','目','亞目','總科','科','亞科','屬','亞屬']: # 
                        if frs.get('family') or frs.get('class'):
                            if frs.get('family') == t_family or frs.get('class') == t_class:
                                filtered_rs.append(frs)
                                has_taxon_parent = True
                    else:
                        has_taxon_parent = False # TODO 這邊先當成沒有nm上階層，直接比對學名
                    # elif t_rank in ['綱','總綱','亞門']: # 還要再往上抓到門
                    # elif t_rank in ['亞界','總門','門']: #  還要再往上抓到界
                # 如果有任何有nm上階層 且filtered_rss > 0 就代表有上階層比對成功的結果
                if has_taxon_parent:
                    if len(filtered_rs) == 1:
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'taxonID'] = filtered_rs[0]['taxonID']
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = None
                    else:
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 4
                else:
                    # 如果沒有任何nm上階層的結果，則直接用filtered_rs
                    if len(matched_taxon) == 1:
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'taxonID'] = matched_taxon[0]['taxonID']
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = None
                    else:
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 4
            # 若沒有上階層資訊，就直接取比對結果
            else:
                if len(matched_taxon) == 1:
                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),'taxonID'] = matched_taxon[0]['taxonID']
                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = None
                else:
                    sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.originalScientificName==original_name)),f'stage_{match_stage}'] = 4

def matching_flow_tbn(sci_names):
    ## 第一階段比對 - sourceScientificName
    # TODO 未來要改成優先採用TaiCOL taxonID (若原資料庫有提供)
    for s in sci_names.index:
        s_row = sci_names.iloc[s]
        if s_row.sourceScientificName:
            match_name_tbn(s_row.sourceScientificName,s_row.sourceScientificName,s_row.originalScientificName,s_row.sourceTaxonID,False,1)
    ## 第二階段比對 沒有taxonID的 試抓TaiCOL namecode
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 2
    no_taxon = sci_names[sci_names.taxonID=='']
    for s in no_taxon.index:
        s_row = sci_names.iloc[s]
        if s_row.scientificNameID:
            match_namecode_tbn(s_row.scientificNameID,s_row.sourceScientificName,s_row.originalScientificName,s_row.sourceTaxonID,2)
    ## 第三階段比對 - originalScientificName 英文比對
    ## 第三階段比對 - originalScientificName 中文比對
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 3
    no_taxon = sci_names[(sci_names.taxonID=='')&(sci_names.originalScientificName!='')]
    # 要判斷是中文還是英文(英文可能帶有標點符號)
    for nti in no_taxon.index:
        nt_str = sci_names.loc[nti,'originalScientificName']
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
                    match_name_tbn(sl, sci_names.loc[nti,'sourceScientificName'],sci_names.loc[nti,'originalScientificName'],sci_names.loc[nti,'sourceTaxonID'],False,3)
                    # 如果對到就break
                    if sci_names.loc[nti,'taxonID']:
                        break
                else:
                    # 中文
                    match_name_tbn(sl, sci_names.loc[nti,'sourceScientificName'],sci_names.loc[nti,'originalScientificName'],sci_names.loc[nti,'sourceTaxonID'],False,3)
    ## 第四階段比對 - scientificName第一個英文單詞 (為了至少可以補階層)
    ## 這個情況要給的是parentTaxonID
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 4
    no_taxon = sci_names[sci_names.taxonID=='']
    for nti in no_taxon.index:
        if nt_str := sci_names.loc[nti,'sourceScientificName']:
            if len(nt_str.split(' ')) > 1: # 等於0的話代表上面已經對過了
                match_name_tbn(nt_str.split(' ')[0], sci_names.loc[nti,'sourceScientificName'],sci_names.loc[nti,'originalScientificName'],sci_names.loc[nti,'sourceTaxonID'],True,4)
    ## 第五階段比對 - originalScientificName第一個英文單詞 (為了至少可以補階層)
    ## 這個情況要給的是parentTaxonID
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 5
    no_taxon = sci_names[(sci_names.taxonID=='')&(sci_names.parentTaxonID=='')]
    for nti in no_taxon.index:
        if nt_str := sci_names.loc[nti,'originalScientificName']:
            if len(nt_str.split(' ')) > 1: # 等於0的話代表上面已經對過了
                # 以TBN的資料來說應該第一個是英文 但再確認一次
                nt_str = sci_names.loc[nti,'originalScientificName']
                # 拿掉階層名
                for v in rank_map.values():
                    nt_str = nt_str.replace(v, '')
                # 拿掉空格
                nt_str = re.sub(' +', ' ', nt_str)
                str_list = nt_str.split(' ')
                # 英文的部分組合起來
                eng_part = ' '.join([s for s in str_list if not any(re.findall(r'[\u4e00-\u9fff]+', s))])
                # if not any(re.findall(r'[\u4e00-\u9fff]+', eng_part)):
                match_name_tbn(eng_part.split(' ')[0], sci_names.loc[nti,'sourceScientificName'],sci_names.loc[nti,'originalScientificName'],sci_names.loc[nti,'sourceTaxonID'],True,5)
    # 確定match_stage
    stage_list = [1,2,3,4,5]
    for i in stage_list[:4]:
        for stg in stage_list[stage_list.index(i)+1:]:
            sci_names.loc[sci_names.match_stage==i,f'stage_{stg}'] = None
    sci_names.loc[(sci_names.match_stage==5)&(sci_names.taxonID==''),'match_stage'] = None
    return sci_names


# TBN match


# sourceScientificName : scientificName 
# sourceVernacularName : vernacularName  # 比對的時候沒有用到
# originalScientificName : originalVernacularName
# sourceTaxonID : taxonUUID 
# scientificNameID : taiCOLNameCode
# tbiaID: id 

# ["tbiaID", "sourceScientificName","sourceVernacularName", "originalScientificName", "sourceTaxonID", "scientificNameID"]

import sqlalchemy as sa
# from sqlalchemy import text

# 先取得全部筆數

# group = 'tbri'
# with db.begin() as conn:
#     qry = sa.text("""select count(*) from records where "group" = '{}'""".format(group))
#     resultset = conn.execute(qry)
#     total_count = resultset.one()

# group_list = ['tbri','gbif','oca']
group_list = ['tbri', 'gbif'] #  859,340

# TODO oca先重新抓過資料之後再更新

# 一次處理10000筆

for group in group_list:
    limit = 10000
    offset = 0
    has_more_data = True
    # print(group)
    while has_more_data:
        print(group, offset)
        with db.begin() as conn:
            qry = sa.text("""select "tbiaID", "occurrenceID", "sourceScientificName","sourceVernacularName", "originalScientificName", "sourceTaxonID", "scientificNameID", 
                        "datasetName", "taxonID" as old_taxon_id, "parentTaxonID" as old_parent_taxon_id from records 
                        where "group" = '{}' limit {} offset {}""".format(group, limit, offset))
            resultset = conn.execute(qry)
            results = resultset.mappings().all()
        if len(results) == limit:
            now = datetime.now()
            df = pd.DataFrame(results)
            df['group'] = group
            df = df.replace({nan: '', None: ''})
            sci_names = df[['sourceScientificName','originalScientificName','sourceTaxonID','scientificNameID']].drop_duplicates().reset_index(drop=True)
            # sci_names['sourceScientificName'] = sci_names['scientificName']
            sci_names['taxonID'] = ''
            sci_names['parentTaxonID'] = ''
            sci_names['match_stage'] = 1
            # 各階段的issue default是沒有對到
            sci_names['stage_1'] = 2
            sci_names['stage_2'] = 2
            sci_names['stage_3'] = 2
            sci_names['stage_4'] = 2
            sci_names['stage_5'] = 2
            sci_names = matching_flow_tbn(sci_names)
            df = df.merge(sci_names)
            df = df.replace({nan: None, '': None})
            match_log = df[['occurrenceID','tbiaID','sourceScientificName','taxonID','parentTaxonID','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group']]
            match_log.loc[match_log.taxonID=='','is_matched'] = False
            match_log.loc[(match_log.taxonID!='')|(match_log.parentTaxonID!=''),'is_matched'] = True
            match_log['match_stage'] = match_log['match_stage'].apply(lambda x: int(x) if x else x)
            match_log['stage_1'] = match_log['stage_1'].apply(lambda x: issue_map[x] if x else x)
            match_log['stage_2'] = match_log['stage_2'].apply(lambda x: issue_map[x] if x else x)
            match_log['stage_3'] = match_log['stage_3'].apply(lambda x: issue_map[x] if x else x)
            match_log['stage_4'] = match_log['stage_4'].apply(lambda x: issue_map[x] if x else x)
            match_log['stage_5'] = match_log['stage_5'].apply(lambda x: issue_map[x] if x else x)
            match_log['created'] = now
            match_log['modified'] = now
            # TODO 未來match_log要改成用更新的
            match_log.to_sql('match_log', db, if_exists='append',index=False)
            # 更新records table, 用tbiaID可以確定是唯一的
            df = df.replace({None: ''})
            df = df[(df.taxonID!=df.old_taxon_id)|(df.parentTaxonID!=df.old_parent_taxon_id)]
            df = df.reset_index(drop=True)
            for i in df.index:
                row = df.iloc[i]
                row = row.replace({'': None})
                # 如果不一樣的話再update?
                stmt = f'UPDATE records SET modified = :modified, "taxonID" = :taxonID, "parentTaxonID" = :parentTaxonID WHERE "tbiaID" = :tbiaID'
                values = {
                    'modified': now,
                    'taxonID': row.taxonID,
                    'parentTaxonID': row.parentTaxonID,
                    'tbiaID': row.tbiaID,
                }
                with db.begin() as conn:
                    a = conn.execute(sa.text(stmt), values)
            offset += limit
        else:
            has_more_data = False




