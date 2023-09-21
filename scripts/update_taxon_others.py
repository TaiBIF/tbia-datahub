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
# # 取得taxon資料
# import psycopg2
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



# others match

def match_name(matching_name,sci_name,original_name,match_stage):
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
                if len(filtered_rs):
                    # 排除掉同個taxonID但有不同name的情況
                    filtered_rs = pd.DataFrame(filtered_rs)[['accepted_namecode']].drop_duplicates().to_dict(orient='records')
                    # NomenMatch 有比對到有效taxon
                    # 沒有上階層資訊，就直接取比對結果
                    if len(filtered_rs) == 1:
                        if match_score < 1:
                            sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),f'stage_{match_stage}'] = 3
                        else:
                            sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),f'stage_{match_stage}'] = None
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),'taxonID'] = filtered_rs[0]['accepted_namecode']
                    else:
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),f'stage_{match_stage}'] = 4


# 國家公園只有1,3,4階段
def matching_flow(sci_names):
    ## 第一階段比對 - scientificName
    # TODO 未來要改成優先採用TaiCOL taxonID (若原資料庫有提供)
    for s in sci_names.index:
        s_row = sci_names.iloc[s]
        if s_row.sourceScientificName:
            match_name(s_row.sourceScientificName,s_row.sourceScientificName,s_row.sourceVernacularName,1)
    # ## 第二階段比對 沒有taxonID的 試抓TaiCOL namecode
    ## 第三階段比對 - sourceVernacularName 中文比對
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 3
    no_taxon = sci_names[(sci_names.taxonID=='')&(sci_names.sourceVernacularName!='')]
    for nti in no_taxon.index:
        # 可能有多個sourceVernacularName
        names = sci_names.iloc[nti].sourceVernacularName
        for nn in names.split(';'):
            if not sci_names.loc[nti,'taxonID']:
                match_name(nn, sci_names.loc[nti,'sourceScientificName'],sci_names.loc[nti,'sourceVernacularName'],3)
    ## 第四階段比對 - scientificName第一個英文單詞 (為了至少可以補階層)
    ## 這個情況要給的是parentTaxonID
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 4
    no_taxon = sci_names[sci_names.taxonID=='']
    for nti in no_taxon.index:
        if nt_str := sci_names.loc[nti,'sourceScientificName']:
            if len(nt_str.split(' ')) > 1: # 等於0的話代表上面已經對過了
                match_name(nt_str.split(' ')[0], sci_names.loc[nti,'sourceScientificName'],sci_names.loc[nti,'sourceVernacularName'],4)
    # 確定match_stage
    stage_list = [1,2,3,4,5]
    for i in stage_list[:4]:
        for stg in stage_list[stage_list.index(i)+1:]:
            sci_names.loc[sci_names.match_stage==i,f'stage_{stg}'] = None
    sci_names.loc[(sci_names.match_stage==4)&(sci_names.taxonID==''),'match_stage'] = None
    return sci_names



# ["tbiaID", "sourceScientificName","sourceVernacularName", "originalScientificName", "sourceTaxonID", "scientificNameID"]


# "forest",1526663,
# "cpami",781492,
# "taif",385669,
# "tcd",208144,
# "fact",175183,
# "brmas",126480,

# group_list = ['forest','cpami','taif','tcd','fact','brmas']

group_list = ['brmas'] 



# 一次處理10000筆

# TODO is_matched的部分要再確認

# 19031165

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
        if len(results):
            now = datetime.now()
            df = pd.DataFrame(results)
            df['group'] = group
            df = df.replace({nan: '', None: ''})
            sci_names = df[['sourceScientificName','sourceVernacularName','sourceTaxonID','scientificNameID']].drop_duplicates().reset_index(drop=True)
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
            sci_names = matching_flow(sci_names)
            df = df.merge(sci_names)
            df = df.replace({nan: None, '': None})
            match_log = df[['occurrenceID','tbiaID','sourceScientificName','taxonID','parentTaxonID','match_stage','stage_1','stage_2','stage_3','stage_4','stage_5','group']]
            match_log['is_matched'] = False
            match_log.loc[match_log.taxonID.notnull(),'is_matched'] = True
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
            match_log.to_csv(f'/portal/media/match_log/{group}_{offset}.csv',index=None)
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
        if len(results) < limit:
            has_more_data = False

 