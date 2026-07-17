#!/usr/bin/env python3
# 資料更新後再更新台灣網格
# python update_tw_grid.py
# 或指定網格清單
# python update_tw_grid.py --grid-csv TW_grid_5.csv
# 資料空缺圖台使用

import argparse
import json

import pandas as pd
import requests
from tqdm import tqdm

SOLR = 'http://solr:8983/solr'
BATCH_GRIDS = 200  # 每累積幾個 grid 就 post 一次

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
    '國家海洋資料庫及共享平台': 'namr',
    '集水區友善環境生態資料庫': 'ardswc',
    '中油生態地圖': 'cpc',
    '作物種原資訊系統': 'npgrc',
    '國立海洋生物博物館生物典藏管理系統': 'nmmba',
    '愛自然-臺灣(iNaturalist Taiwan)': 'ntuforestry',
}

rights_holder_map = {
    'GBIF': '1',
    '中央研究院生物多樣性中心動物標本館': '2',
    '中央研究院生物多樣性中心植物標本資料庫': '3',
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
    '國家海洋資料庫及共享平台': '16',
    '集水區友善環境生態資料庫': '17',
    '中油生態地圖': '18',
    '作物種原資訊系統': '19',
    '國立海洋生物博物館生物典藏管理系統': '20',
    '愛自然-臺灣(iNaturalist Taiwan)': '21',
}

taxon_group_map_e = {
    '鳥類': '1',
    '爬蟲類': '2',
    '哺乳類': '3',
    '甲蟲類': '4',
    '魚類': '5',
    '兩棲類': '6',
    '蛾類': '7',
    '蝶類': '8',
    '蜘蛛': '9',
    '蜻蛉類': '10',
    '蝸牛與貝類': '11',
    '其他昆蟲': '12',
    '蝦蟹類': '13',
    '裸子植物': '14',
    '被子植物': '15',
    '蕨類植物': '16',
    '苔蘚植物': '17',
    '真菌': '18',
    '藻類': '19',
    '病毒': '20',
    '細菌': '21',
}

RANK_FILTER = 'taxonRank:(species OR subspecies OR nothosubspecies OR variety OR subvariety OR nothovariety OR form OR subform OR "special form" OR race OR stirp OR morph OR aberration)'


def fetch_counts(field):
    """一次 pivot query 撈回所有 grid 的計數，只存非 0 者：{(grid, bioGroup, rightsHolder): count}"""
    query = {
        "query": "*:*",
        "limit": 0,
        "filter": ['-standardOrganismQuantity:0', RANK_FILTER, 'is_in_taiwan:true'],
    }
    pivot_key = f'{field},bioGroup,rightsHolder'
    url = (f'{SOLR}/tbia_records/select?facet.pivot={pivot_key}'
           '&facet.limit=-1&facet.mincount=1&facet=true')
    resp = requests.post(url, data=json.dumps(query),
                         headers={'content-type': 'application/json'}).json()

    counts = {}
    for g in resp['facet_counts']['facet_pivot'][pivot_key]:
        grid = g['value']
        for b in g.get('pivot', []):
            bio = b['value']
            for h in b.get('pivot', []):
                counts[(grid, bio, h['value'])] = h['count']
    return counts


def post_docs(docs):
    """把一批文件以固定 id upsert 進 Solr（相同 id 覆蓋）"""
    if not docs:
        return
    resp = requests.post(f'{SOLR}/tw_grid/update?commit=true',
                         data=json.dumps(docs),
                         headers={'content-type': 'application/json'})
    resp.raise_for_status()


def sync(field, is_blur, grids):
    counts = fetch_counts(field)
    suffix = '_blur' if is_blur else ''
    desc = 'blurred' if is_blur else 'exact'

    batch = []
    for i, grid in enumerate(tqdm(grids, desc=desc), 1):
        grid_x, grid_y = grid.split('_')
        for bio, bio_id in taxon_group_map_e.items():
            for holder, holder_id in rights_holder_map.items():
                batch.append({
                    'id': f'{grid}_{bio_id}_{holder_id}{suffix}',
                    'bioGroup': bio,
                    'rightsHolder': holder,
                    'total_count': counts.get((grid, bio, holder), 0),
                    'grid_5': grid,
                    'grid_x': grid_x,
                    'grid_y': grid_y,
                    'group': rights_holder_group[holder],
                    'is_blurred': is_blur,
                })
        if i % BATCH_GRIDS == 0:
            post_docs(batch)
            batch = []
    post_docs(batch)  # 收尾


def main():
    parser = argparse.ArgumentParser(description='更新台灣網格 Solr 資料（固定 id 更新）')
    parser.add_argument('--grid-csv', default='TW_grid_5.csv', help='網格清單 CSV')
    args = parser.parse_args()

    grids = pd.read_csv(args.grid_csv).grid_5.unique()  # 3756
    sync('grid_5', False, grids)
    sync('grid_5_blurred', True, grids)
    print('Solr 更新完成')


if __name__ == '__main__':
    main()
