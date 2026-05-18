import sqlite3
import os
import pandas as pd
import requests
import json
from numpy import nan
import concurrent.futures
from scripts.utils.common import to_none_dict
from scripts.utils.progress import timed

class DedupTracker:
    """
    使用 SQLite 追蹤已處理的 (datasetName, occurrenceID/catalogNumber)，
    支援批內去重與跨批去重，並累積重複紀錄供最後輸出 CSV。
    """

    TIME_COLS = {'created', 'modified', 'sourceCreated', 'sourceModified'}

    def __init__(self, rights_holder, update_version,
                 cache_dir='/bucket/dedup_cache'):
        self.rights_holder = rights_holder
        self.update_version = update_version
        self.duplicates = []

        os.makedirs(cache_dir, exist_ok=True)
        safe_name = rights_holder.replace(' ', '_').replace('/', '_')
        self.db_path = os.path.join(
            cache_dir, f'{safe_name}_{update_version}.sqlite')
        self._init_db()

    # ── SQLite 初始化 ──────────────────────────────────────────

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            # 偵測舊 schema（有 key_field 欄位）→ 直接 drop 重建
            cursor = conn.execute("PRAGMA table_info(seen_keys)")
            cols = [row[1] for row in cursor.fetchall()]
            if cols and 'key_field' in cols:
                conn.execute('DROP TABLE seen_keys')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS seen_keys (
                    dataset_name    TEXT NOT NULL,
                    occurrence_id   TEXT NOT NULL DEFAULT '',
                    catalog_number  TEXT NOT NULL DEFAULT '',
                    tbia_id         TEXT NOT NULL,
                    PRIMARY KEY (dataset_name, occurrence_id, catalog_number)
                )
            ''')

    # ── 1. 批內去重 ───────────────────────────────────────────

    def dedup_within_batch(self, df):
        """
        同一批 df 內，依 (datasetName, occurrenceID, catalogNumber) 完全相同視為重複，
        只保留第一筆並記錄。空值會和其他空值匹配，但與任何非空值都不匹配。
        occurrenceID 和 catalogNumber 都為空的記錄一律保留（無法辨識）。
        回傳去重後的 df。
        """
        if df.empty:
            return df

        compare_cols = [c for c in df.columns
                        if c not in self.TIME_COLS and c != 'id']

        # 建立 normalized 的 key 欄位（trim 後的字串），確保 '' 和缺欄位都當作空
        key_df = pd.DataFrame(index=df.index)
        key_df['datasetName'] = df.get('datasetName', '').astype(str)
        for col in ['occurrenceID', 'catalogNumber']:
            if col in df.columns:
                key_df[col] = df[col].astype(str).str.strip()
            else:
                key_df[col] = ''

        # 只考慮至少有一個 identifier 的 rows
        has_id = (key_df['occurrenceID'] != '') | (key_df['catalogNumber'] != '')
        subset_keys = key_df[has_id]
        if subset_keys.empty:
            return df

        group_cols = ['datasetName', 'occurrenceID', 'catalogNumber']
        dup_mask = subset_keys.duplicated(subset=group_cols, keep=False)
        if not dup_mask.any():
            return df

        to_remove = set()

        for (ds, occ_id, cat_num), grp in subset_keys[dup_mask].groupby(group_cols):
            # 比對非時間欄位
            grp_full = df.loc[grp.index]
            grp_cmp = grp_full[compare_cols].astype(str)
            fields_identical = len(grp_cmp.drop_duplicates()) == 1

            differing = ''
            if not fields_identical:
                first = grp_cmp.iloc[0]
                diffs = set()
                for _, row in grp_cmp.iloc[1:].iterrows():
                    diffs.update(
                        c for c in compare_cols if first[c] != row[c])
                differing = ','.join(sorted(diffs))

            keep_idx = grp.index[0]
            for idx in grp.index[1:]:
                if idx in to_remove:
                    continue
                to_remove.add(idx)
                self.duplicates.append({
                    'rightsHolder': self.rights_holder,
                    'datasetName': ds,
                    'occurrenceID': occ_id,
                    'catalogNumber': cat_num,
                    'duplicate_type': 'within_batch',
                    'fields_identical': fields_identical,
                    'differing_fields': differing,
                    'kept_id': df.at[keep_idx, 'id'],
                    'removed_id': df.at[idx, 'id'],
                })

        if to_remove:
            print(f'   ⚠️ 批內去重：移除 {len(to_remove)} 筆重複')
            df = df.drop(index=to_remove).reset_index(drop=True)

        return df

    # ── 2. 跨批去重 ───────────────────────────────────────────

    def find_cross_batch_supplement(self, df, existed_records):
        """
        查 SQLite 找出已在先前批次處理、但 Solr 尚未收錄的 records。
        依 (datasetName, occurrenceID, catalogNumber) 完整 tuple 比對。
        回傳補充用的 DataFrame（格式同 existed_records）。
        """
        if df.empty:
            return pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])

        already_found = set()
        if existed_records is not None and not existed_records.empty:
            already_found = set(existed_records['tbiaID'].tolist())

        # 建立 normalized 的 key 欄位
        key_df = pd.DataFrame(index=df.index)
        key_df['datasetName'] = df.get('datasetName', '').astype(str)
        for col in ['occurrenceID', 'catalogNumber']:
            if col in df.columns:
                key_df[col] = df[col].astype(str).str.strip()
            else:
                key_df[col] = ''

        # 只查至少有一個 identifier 的 rows
        has_id = (key_df['occurrenceID'] != '') | (key_df['catalogNumber'] != '')
        subset = key_df[has_id]
        if subset.empty:
            return pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])

        # 取出 unique tuples，避免重複查詢
        unique_tuples = subset[['datasetName', 'occurrenceID', 'catalogNumber']].drop_duplicates()

        supplement = []
        batch_size = 500

        with sqlite3.connect(self.db_path) as conn:
            tuples_list = list(unique_tuples.itertuples(index=False, name=None))
            # 分批：每批用 OR 串接 tuple 比對
            for i in range(0, len(tuples_list), batch_size):
                batch = tuples_list[i:i + batch_size]
                conditions = ' OR '.join(
                    ['(dataset_name = ? AND occurrence_id = ? AND catalog_number = ?)'] * len(batch)
                )
                params = [v for tup in batch for v in tup]
                rows = conn.execute(
                    f'SELECT dataset_name, occurrence_id, catalog_number, tbia_id '
                    f'FROM seen_keys WHERE {conditions}',
                    params,
                ).fetchall()

                for ds_name, occ_id, cat_num, tbia_id in rows:
                    if tbia_id in already_found:
                        continue  # Solr 已回傳，不用補
                    already_found.add(tbia_id)

                    # 找 df 中對應的行記錄為 duplicate
                    matched_mask = (
                        (subset['datasetName'] == ds_name)
                        & (subset['occurrenceID'] == occ_id)
                        & (subset['catalogNumber'] == cat_num)
                    )
                    for idx in subset[matched_mask].index:
                        self.duplicates.append({
                            'rightsHolder': self.rights_holder,
                            'datasetName': ds_name,
                            'occurrenceID': occ_id,
                            'catalogNumber': cat_num,
                            'duplicate_type': 'cross_batch',
                            'fields_identical': '',
                            'differing_fields': '',
                            'kept_id': tbia_id,
                            'removed_id': df.at[idx, 'id'],
                        })

                    supplement.append({
                        'tbiaID': tbia_id,
                        'occurrenceID': occ_id,
                        'catalogNumber': cat_num,
                    })

        if supplement:
            result = pd.DataFrame(supplement).drop_duplicates()
            print(f'   ⚠️ 跨批重複：發現 {len(result)} 筆已在先前批次處理過')
            return result

        return pd.DataFrame(columns=['tbiaID', 'occurrenceID', 'catalogNumber'])

    # ── 3. 記錄已處理的 key ────────────────────────────────────

    def record_batch_keys(self, df):
        """
        upsert 成功後，把該批的 key 寫入 SQLite。
        df 此時應有 tbiaID, datasetName, occurrenceID, catalogNumber。
        """
        if df.empty:
            return

        tbia_col = 'tbiaID' if 'tbiaID' in df.columns else 'id'
        records = []
        for _, row in df.iterrows():
            tbia_id = str(row.get(tbia_col, ''))
            ds_name = str(row.get('datasetName', ''))
            occ_id = str(row.get('occurrenceID', '')).strip() if 'occurrenceID' in df.columns else ''
            cat_num = str(row.get('catalogNumber', '')).strip() if 'catalogNumber' in df.columns else ''
            # 過濾掉 'None'、'nan' 等假空值
            if occ_id in ('None', 'nan'):
                occ_id = ''
            if cat_num in ('None', 'nan'):
                cat_num = ''
            # 至少有一個 identifier 才寫入
            if occ_id or cat_num:
                records.append((ds_name, occ_id, cat_num, tbia_id))

        if records:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany('''
                    INSERT OR REPLACE INTO seen_keys
                    (dataset_name, occurrence_id, catalog_number, tbia_id)
                    VALUES (?, ?, ?, ?)
                ''', records)

    # ── 4. 輸出重複紀錄 CSV ────────────────────────────────────

    def export_duplicates_csv(self,
                              output_dir='/portal/media/duplicate_logs'):
        os.makedirs(output_dir, exist_ok=True)
        if self.duplicates:
            safe_name = self.rights_holder.replace(' ', '_').replace('/', '_')
            path = os.path.join(
                output_dir,
                f'duplicates_{safe_name}_{self.update_version}.csv')
            pd.DataFrame(self.duplicates).to_csv(path, index=False)
            print(f'📋 重複紀錄已輸出：{path}（共 {len(self.duplicates)} 筆）')
        else:
            print('✅ 本次無重複紀錄')

    # ── 5. 清理 SQLite（整個 run 結束後可選呼叫）───────────────

    def cleanup_cache(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
            print(f'🗑️ 已清除 dedup cache: {self.db_path}')

@timed()
def resolve_existed_records(df, rights_holder, dedup_tracker):
    """
    批內去重、查詢已存在記錄、跨批去重補充，並回寫 tbiaID。
    """
    df = df.copy()
    for col in ['occurrenceID', 'catalogNumber']:
        if col in df.columns:
            df[col] = df[col].astype(str)
        else:
            df[col] = ''

    df = dedup_tracker.dedup_within_batch(df)

    occ_ids = df.loc[df.occurrenceID != '', 'occurrenceID'].tolist()
    cata_ids = df.loc[df.catalogNumber != '', 'catalogNumber'].tolist()
    existed_records = get_existed_records_optimized(
        occ_ids=occ_ids, rights_holder=rights_holder, cata_ids=cata_ids
    ).replace({nan: ''})

    cross_supplement = dedup_tracker.find_cross_batch_supplement(df, existed_records)
    if not cross_supplement.empty:
        existed_records = pd.concat(
            [existed_records, cross_supplement]).drop_duplicates(subset=['tbiaID'])

    if len(existed_records):
        df = df.merge(existed_records, how='left').replace(to_none_dict)
        df['id'] = df['tbiaID'].fillna(df['id'])
        df = df.drop(columns=['tbiaID'])

    return df, existed_records


def get_existed_records_optimized(occ_ids, rights_holder, get_reference=False, cata_ids=[],
                                 batch_size=500, max_workers=4):
    get_fields = ['id', 'occurrenceID', 'catalogNumber']
    if get_reference:
        get_fields.append('references')

    def query_batch(ids, field_name):
        if not ids:
            return []
        query = {
            "query": "*:*",
            "offset": 0,
            "filter": [
                f'rightsHolder:"{rights_holder}"',
                f'{{!terms f={field_name}}}{",".join(ids)}'
            ],
            "limit": len(ids) * 2,
            "fields": get_fields
        }
        try:
            resp = requests.post(
                'http://solr:8983/solr/tbia_records/select',
                data=json.dumps(query),
                headers={'content-type': "application/json"},
                timeout=30
            ).json()
            return resp.get('response', {}).get('docs', [])
        except Exception as e:
            print(f"Query error: {e}")
            return []

    tasks = (
        [(occ_ids[i:i+batch_size], 'occurrenceID') for i in range(0, len(occ_ids), batch_size)] +
        [(cata_ids[i:i+batch_size], 'catalogNumber') for i in range(0, len(cata_ids), batch_size)]
    )

    subset_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(query_batch, ids, field) for ids, field in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                subset_list.extend(future.result())
            except Exception as e:
                print(f"Future error: {e}")

    if not subset_list:
        return pd.DataFrame(columns=get_fields).rename(columns={'id': 'tbiaID'})

    result_df = pd.DataFrame(subset_list).rename(columns={'id': 'tbiaID'})
    result_cols = ['tbiaID' if c == 'id' else c for c in get_fields]
    for col in ['tbiaID', 'occurrenceID', 'catalogNumber']:
        if col not in result_df.columns:
            result_df[col] = ''
    return result_df[result_cols].drop_duplicates()