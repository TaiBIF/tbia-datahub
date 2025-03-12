# 資料更新後再更新台灣網格

import pandas as pd
import requests
import json

df = pd.read_csv('TW_grid_5.csv')



rights_holder_group = {
    'GBIF': 'gbif',
    '中央研究院生物多樣性中心動物標本館': 'asiz',
    '中央研究院生物多樣性中心植物標本資料庫': 'hast',
    '台灣生物多樣性網絡 TBN': 'tbri',
    '國立臺灣博物館典藏': 'ntm',
    '林業試驗所昆蟲標本館': 'fact',
    '林業試驗所植物標本資料庫': 'taif',
    '河川環境資料庫': 'wra',
    '濕地環境資料庫': 'nps',
    '生態調查資料庫系統': 'forest',
    '臺灣國家公園生物多樣性資料庫': 'nps',
    '臺灣生物多樣性資訊機構 TaiBIF': 'brcas',
    '海洋保育資料倉儲系統': 'oca',
    '科博典藏 (NMNS Collection)': 'nmns',
    '臺灣魚類資料庫': 'ascdc',
}

rights_holder_map = {
    'GBIF': '1',
    '中央研究院生物多樣性中心植物標本資料庫': '2',
    '中央研究院生物多樣性中心動物標本館': '3',
    '台灣生物多樣性網絡 TBN': '4',
    '國立臺灣博物館典藏': '5',
    '林業試驗所昆蟲標本館': '6',
    '林業試驗所植物標本資料庫': '7',
    '河川環境資料庫': '8',
    '濕地環境資料庫': '9',
    '生態調查資料庫系統': '10',
    '臺灣國家公園生物多樣性資料庫': '11',
    '臺灣生物多樣性資訊機構 TaiBIF': '12',
    '海洋保育資料倉儲系統': '13',
    '科博典藏 (NMNS Collection)': '14',
    '臺灣魚類資料庫': '15',
}


taxon_group_map_e = {
    "昆蟲": "1",
    "蜘蛛": "2",
    "魚類": "3",
    "爬蟲類": "4",
    "兩棲類": "5",
    "鳥類": "6",
    "哺乳類": "7",
    "維管束植物": "8",
    "蕨類植物": "9",
    "苔蘚植物": "10",
    "藻類": "11",
    "病毒": "12",
    "細菌": "13",
    "真菌": "14",
    "其他": "15"
}



# grid_5 & grid_5_blurred要各自query一次
# query_list = ['-standardOrganismQuantity:0', 'taxonRank:(species OR subspecies OR nothosubspecies OR variety OR subvariety OR nothovariety OR form OR subform OR "special form" OR race OR stirp OR morph OR aberration)', 'is_in_taiwan=1']


# if req_dict.get('from') == 'datagap':
#     query_list += [f'taxonRank:(species OR subspecies OR nothosubspecies OR variety OR subvariety OR nothovariety OR form OR subform OR "special form" OR race OR stirp OR morph OR aberration)']


# 是資料筆數不是individualCount
c = 0
total_results = []


for grid in df.grid_5.unique(): # 3756
    c += 1
    print(c)
    results = []
    query = { "query": "*:*",
        "limit": 0,
        "filter": ['grid_5:{}'.format(grid),'-standardOrganismQuantity:0', 'taxonRank:(species OR subspecies OR nothosubspecies OR variety OR subvariety OR nothovariety OR form OR subform OR "special form" OR race OR stirp OR morph OR aberration)', 'is_in_taiwan:true'],
    }
    response = requests.post('http://solr:8983/solr/tbia_records/select?facet.pivot=bioGroup,rightsHolder&facet.limit=-1&facet.mincount=1&facet=true', data=json.dumps(query), headers={'content-type': "application/json" })
    resp = response.json()
    # facets = resp['facet_counts']['facet_fields']
    if resp['facet_counts']['facet_pivot']['bioGroup,rightsHolder']:
        data = resp['facet_counts']['facet_pivot']['bioGroup,rightsHolder']
        for d in data:
            now_bio_group = d['value']
            for p in d['pivot']:
                now_rightsHolder = p['value']
                now_count = p['count']
                results.append({'bioGroup': now_bio_group, 'rightsHolder': now_rightsHolder, 'total_count': now_count, 'grid_5': grid, 'is_blurred': False, 'id': '{}_{}_{}'.format(grid, taxon_group_map_e[now_bio_group], rights_holder_map[now_rightsHolder])})
    query = { "query": "*:*",
        "limit": 0,
        "filter": ['grid_5:{}'.format(grid),'-bioGroup:*','-standardOrganismQuantity:0', 'taxonRank:(species OR subspecies OR nothosubspecies OR variety OR subvariety OR nothovariety OR form OR subform OR "special form" OR race OR stirp OR morph OR aberration)', 'is_in_taiwan:true'],
    }
    response = requests.post('http://solr:8983/solr/tbia_records/select?facet.field=rightsHolder&facet.limit=-1&facet.mincount=1&facet=true', data=json.dumps(query), headers={'content-type': "application/json" })
    resp = response.json()
    data = resp['facet_counts']['facet_fields']['rightsHolder']
    for ll in range(0, len(data), 2):
        now_bio_group = '其他'
        now_rightsHolder = data[ll]
        now_count = data[ll+1]
        results.append({'bioGroup': now_bio_group, 'rightsHolder': now_rightsHolder, 'total_count': now_count, 'grid_5': grid, 'is_blurred': False, 'id': '{}_{}_{}'.format(grid, taxon_group_map_e[now_bio_group], rights_holder_map[now_rightsHolder])})
    # 補上0的資料
    now_df = pd.DataFrame(results, columns=['bioGroup','rightsHolder','total_count','grid_5'])
    for now_bio_group in taxon_group_map_e.keys():
        for now_rightsHolder in rights_holder_map.keys():
            if not len(now_df[(now_df.bioGroup==now_bio_group)&(now_df.rightsHolder==now_rightsHolder)]):
                results.append({'bioGroup': now_bio_group, 'rightsHolder': now_rightsHolder, 'total_count': 0, 'grid_5': grid, 'is_blurred': False, 'id': '{}_{}_{}'.format(grid, taxon_group_map_e[now_bio_group], rights_holder_map[now_rightsHolder])})
    total_results += results



c = 0

for grid in df.grid_5.unique(): # 3756
    c += 1
    print(c)
    results = []
    query = { "query": "*:*",
        "limit": 0,
        "filter": ['grid_5_blurred:{}'.format(grid),'-standardOrganismQuantity:0', 'taxonRank:(species OR subspecies OR nothosubspecies OR variety OR subvariety OR nothovariety OR form OR subform OR "special form" OR race OR stirp OR morph OR aberration)', 'is_in_taiwan:true'],
    }
    response = requests.post('http://solr:8983/solr/tbia_records/select?facet.pivot=bioGroup,rightsHolder&facet.limit=-1&facet.mincount=1&facet=true', data=json.dumps(query), headers={'content-type': "application/json" })
    resp = response.json()
    # facets = resp['facet_counts']['facet_fields']
    if resp['facet_counts']['facet_pivot']['bioGroup,rightsHolder']:
        data = resp['facet_counts']['facet_pivot']['bioGroup,rightsHolder']
        for d in data:
            now_bio_group = d['value']
            for p in d['pivot']:
                now_rightsHolder = p['value']
                now_count = p['count']
                results.append({'bioGroup': now_bio_group, 'rightsHolder': now_rightsHolder, 'total_count': now_count, 'grid_5': grid, 'is_blurred': True, 'id': '{}_{}_{}_blur'.format(grid, taxon_group_map_e[now_bio_group], rights_holder_map[now_rightsHolder])})
    query = { "query": "*:*",
        "limit": 0,
        "filter": ['grid_5:{}'.format(grid),'-bioGroup:*','-standardOrganismQuantity:0', 'taxonRank:(species OR subspecies OR nothosubspecies OR variety OR subvariety OR nothovariety OR form OR subform OR "special form" OR race OR stirp OR morph OR aberration)', 'is_in_taiwan:true'],
    }
    response = requests.post('http://solr:8983/solr/tbia_records/select?facet.field=rightsHolder&facet.limit=-1&facet.mincount=1&facet=true', data=json.dumps(query), headers={'content-type': "application/json" })
    resp = response.json()
    data = resp['facet_counts']['facet_fields']['rightsHolder']
    for ll in range(0, len(data), 2):
        now_bio_group = '其他'
        now_rightsHolder = data[ll]
        now_count = data[ll+1]
        results.append({'bioGroup': now_bio_group, 'rightsHolder': now_rightsHolder, 'total_count': now_count, 'grid_5': grid, 'is_blurred': True, 'id': '{}_{}_{}_blur'.format(grid, taxon_group_map_e[now_bio_group], rights_holder_map[now_rightsHolder])})
    # 補上0的資料
    now_df = pd.DataFrame(results, columns=['bioGroup','rightsHolder','total_count','grid_5'])
    for now_bio_group in taxon_group_map_e.keys():
        for now_rightsHolder in rights_holder_map.keys():
            if not len(now_df[(now_df.bioGroup==now_bio_group)&(now_df.rightsHolder==now_rightsHolder)]):
                results.append({'bioGroup': now_bio_group, 'rightsHolder': now_rightsHolder, 'total_count': 0, 'grid_5': grid, 'is_blurred': True, 'id': '{}_{}_{}_blur'.format(grid, taxon_group_map_e[now_bio_group], rights_holder_map[now_rightsHolder])})
    total_results += results





import subprocess
import datetime

# commands = f''' curl http://solr:8983/solr/tw_grid/update/?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>*:*</query></delete>'; ''' 
# process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# a = process.communicate()


# 直接用固定id更新

today = datetime.datetime.now().strftime('%Y%m%d')

total_df = pd.DataFrame(total_results)
total_df['grid_x'] = total_df['grid_5'].apply(lambda x: x.split('_')[0]) 
total_df['grid_y'] = total_df['grid_5'].apply(lambda x: x.split('_')[1]) 
total_df['group'] = total_df['rightsHolder'].apply(lambda x: rights_holder_group[x]) 

updating_csv = '/bucket/solr_tw_grid_{}.csv'.format(today)
total_df.to_csv(updating_csv, index=None, escapechar='\\')


commands = f''' curl http://solr:8983/solr/tw_grid/update/?commit=true -H "Content-Type: text/csv" --data-binary @{updating_csv}; ''' 
process = subprocess.Popen(commands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
a = process.communicate()