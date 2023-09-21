# 重新比對的時候要抓回原始檔

import requests
import pandas as pd

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
import sqlalchemy as sa

import requests



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


# # TODO portal & datahub兩邊還是會互相影響 有可能用另外的方法嗎
# conn = psycopg2.connect(**portal_db_settings)

# cur = conn.cursor()
# cur.execute('SELECT * FROM data_taxon')
# taxon = cur.fetchall()
# colnames = [desc[0] for desc in cur.description]
# taxon = pd.DataFrame(taxon, columns=colnames)
# sub_col = [c for c in colnames if c not in ['id', 'iucn', 'redlist', 'protected', 'sensitive', 'cites']]
# taxon = taxon[sub_col]



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
                    # TODO 這邊要確認上階層資訊
                    # 沒有上階層資訊，就直接取比對結果
                    if len(filtered_rs) == 1:
                        if match_score < 1:
                            sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),f'stage_{match_stage}'] = 3
                        else:
                            sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),f'stage_{match_stage}'] = None
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),'taxonID'] = filtered_rs[0]['accepted_namecode']
                    else:
                        sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.sourceVernacularName==original_name)),f'stage_{match_stage}'] = 4



def match_namecode(matching_namecode,sci_name,match_stage):
    # 這邊不會有fuzzy的問題 因為直接用namecode對應
    try:
        matching_namecode = str(int(matching_namecode))
    except:
        pass
    # 改成用TaiCOL API
    # taxon_name_id = None
    taxon_data = []
    name_res = requests.get(f'https://api.taicol.tw/v2/namecode?namecode={matching_namecode}')
    # TODO 這邊回傳的資料有修改
    if name_res.status_code == 200:
        if name_data := name_res.json().get('data'):
            # for d in name_data:
            #     print(d)
            # tmp = [d['taxon'] for d in name_data if d.get('usage_status') !='misapplied']
            for n in name_data:
                for tt in n.get('taxon'):
                    if tt.get('usage_status') != 'misapplied':
                        taxon_data.append(tt.get('taxon_id'))
            if len(taxon_data) == 1:
                sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.scientificNameID==matching_namecode)),'taxonID'] = taxon_data[0]
                sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.scientificNameID==matching_namecode)),f'stage_{match_stage}'] = None
            else:
                sci_names.loc[((sci_names.sourceScientificName==sci_name)&(sci_names.scientificNameID==matching_namecode)),f'stage_{match_stage}'] = 4


def matching_flow(sci_names):
    ## 第一階段比對 - scientificName
    # TODO 未來要改成優先採用TaiCOL taxonID (若原資料庫有提供)
    for s in sci_names.index:
        s_row = sci_names.iloc[s]
        if s_row.sourceScientificName:
            match_name(s_row.sourceScientificName,s_row.sourceScientificName,s_row.sourceVernacularName,1)
    ## 第二階段比對 沒有taxonID的 試抓TaiCOL namecode
    sci_names.loc[sci_names.taxonID=='','match_stage'] = 2
    no_taxon = sci_names[sci_names.taxonID=='']
    for s in no_taxon.index:
        s_row = sci_names.iloc[s]
        if s_row.scientificNameID:
            match_namecode(s_row.scientificNameID,s_row.sourceScientificName,2)
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



limit = 10000
offset = 0
has_more_data = True
group = 'oca'
# print(group)

# 先不管高階層

oca_df = pd.read_csv('/portal/bucket/oca_20230915.csv')
oca_df = oca_df[['sourceScientificName','scientificNameID']]
oca_df['scientificNameID'] = oca_df.scientificNameID.str.replace('\t','',regex=False)
oca_df = oca_df.replace({nan: ''})
# oca_df = oca_df.reset_index(drop=True)
oca_df = oca_df.drop_duplicates()
oca_df = oca_df.reset_index(drop=True)

# for o in oca_df.index:
#     row = oca_df.iloc[o]

mmm = pd.DataFrame()
while has_more_data:
    print(offset)
    solr_url = f"http://solr:8983/solr/tbia_records/select?indent=true&rows=10000&start={offset}&q.op=OR&q=group:{group}"
    resp = requests.get(solr_url)
    resp = resp.json()
    results = resp['response']['docs']
    # with db.begin() as conn:
    #     # 可能要改從solr query?
    #     qry = sa.text("""select "tbiaID", "occurrenceID", "sourceScientificName","sourceVernacularName", "scientificNameID", 
    #                 "datasetName", "taxonID" as old_taxon_id, "parentTaxonID" as old_parent_taxon_id from records 
    #                 where "group" = '{}' limit {} offset {}""".format(group, limit, offset))
    #     resultset = conn.execute(qry)
    #     results = resultset.mappings().all()
    if len(results):
        now = datetime.now()
        df = pd.DataFrame(results)
        df['group'] = group
        df = df.replace({nan: '', None: ''})
        df = df.merge(oca_df, how='left')
        sci_names = df[['sourceScientificName','sourceVernacularName','scientificNameID']].drop_duplicates().reset_index(drop=True)
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
        # mmm = pd.concat([match_log, mmm])
        # print(len(match_log))
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




