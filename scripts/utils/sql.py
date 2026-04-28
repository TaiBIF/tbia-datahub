import time
import pandas as pd
from sqlalchemy import text, inspect
from app import engine
"""
批次處理最佳化方案
解決重複更新問題，大幅提升效能

主要改進：
1. 避免重複 UPSERT 操作
2. 批次處理減少資料庫交互
3. 智能判斷新增 vs 更新
4. 減少索引掃描次數
"""



# 建立 inspector
inspector = inspect(engine)
# 直接取得 records 資料表的所有欄位資訊，並取出欄位名稱
records_cols = [col['name'] for col in inspector.get_columns('records')]

def prepare_df_for_sql(df, update_version):
    df = df.copy()
    df['is_deleted'] = False
    df['update_version'] = int(update_version)
    df = df.rename(columns={'id': 'tbiaID'})
    # df = df.drop(columns=[ck for ck in df.keys() if ck not in records_cols],errors='ignore')
    # 直接取交集，只保留在 records_cols 裡面的欄位
    columns_to_keep = df.columns.intersection(records_cols)
    df = df[columns_to_keep]
    return df


def _safe_dollar_quote(value):
    """安全的 PostgreSQL dollar quoting，處理 $v$、\0"""
    s = str(value)
    s = s.replace('\0', '')           # 移除 null byte
    s = s.replace('$v$', '$v$$v$')    # 跳脫 dollar quote tag
    return f"$v${s}$v$"


class OptimizedRecordsProcessor:
    """最佳化的 Records 處理器"""
    
    def __init__(self, db_engine, batch_size=200):
        self.db = db_engine
        self.batch_size = batch_size
        self.failed_records = []
        self._column_types_cache = {}

    def smart_upsert_records(self, df, existed_records=None, table_name='records'):
        """
        智能 UPSERT：使用已取得的existed_records，避免重複查詢
        
        Args:
            df: 要處理的資料
            existed_records: 已存在的記錄(從get_existed_records_optimized取得)
            table_name: 目標表名
        """
        if df.empty:
            return
            
        # print(f"🔄 Processing {len(df)} records with smart upsert...")
        start_time = time.time()
        
        # 1. 使用已取得的existed_records（避免重複查詢）
        if existed_records is not None and not existed_records.empty:
            existing_ids = set(existed_records['tbiaID'].tolist())
            print(f"   📋 Using existing records info: {len(existing_ids)} existed")
        else:
            existing_ids = set()
            print(f"   📋 No existing records provided - treating all as new")
        
        # 2. 分離新增和更新資料
        new_records = df[~df['tbiaID'].isin(existing_ids)].copy()
        update_records = df[df['tbiaID'].isin(existing_ids)].copy()
        
        print(f"   📝 New records: {len(new_records)}")
        print(f"   🔄 Update records: {len(update_records)}")
        
        # 3. 批次新增（使用 COPY）
        if not new_records.empty:
            insert_start = time.time()
            self._copy_insert_records(new_records, table_name)
            # print(f"   ✅ Inserted {len(new_records)} records in {time.time() - insert_start:.2f}s")
        
        # 4. 批次更新
        if not update_records.empty:
            update_start = time.time()
            self._batch_update_records(update_records, table_name)
            # print(f"   ✅ Updated {len(update_records)} records in {time.time() - update_start:.2f}s")
        
        total_time = time.time() - start_time
        rate = len(df) / total_time if total_time > 0 else 0
        print(f"🎯 Smart upsert completed: {len(df)} records in {total_time:.2f}s ({rate:.0f} records/sec)")

    def _copy_insert_records(self, df, table_name):
        """使用 COPY 協議批次新增，比 to_sql 快 5-7 倍"""
        import io

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)

        col_list = ', '.join([f'"{c}"' for c in df.columns])
        copy_sql = f"COPY {table_name} ({col_list}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"

        raw_conn = self.db.raw_connection()
        try:
            cur = raw_conn.cursor()
            cur.copy_expert(copy_sql, buffer)
            raw_conn.commit()
        except Exception as e:
            raw_conn.rollback()
            print(f"     ⚠️ COPY 失敗，回退到 to_sql: {e}")
            df.to_sql(
                table_name, self.db, if_exists='append',
                index=False, chunksize=self.batch_size, method='multi'
            )
        finally:
            raw_conn.close()

    def _get_column_types(self, table_name):
        """從資料庫schema獲取欄位的實際資料類型（有快取）"""
        if table_name in self._column_types_cache:
            return self._column_types_cache[table_name]
        try:
            query = f"""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            AND table_schema = 'public'
            ORDER BY column_name;
            """
            
            with self.db.connect() as conn:
                result = conn.execute(text(query))
                columns_info = result.fetchall()
            
            # 建立欄位類型對應
            column_types = {}
            for col_name, data_type, udt_name in columns_info:
                if data_type in ['timestamp', 'timestamp with time zone', 'timestamp without time zone']:
                    column_types[col_name] = 'timestamp'
                elif data_type in ['integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision']:
                    column_types[col_name] = 'numeric'
                elif data_type == 'boolean':
                    column_types[col_name] = 'boolean'
                elif data_type in ['text', 'character varying', 'varchar', 'char']:
                    column_types[col_name] = 'text'
                elif udt_name == 'geometry':
                    column_types[col_name] = 'geometry'
                else:
                    column_types[col_name] = 'text'  # 預設為文字
            
            self._column_types_cache[table_name] = column_types
            return column_types
            
        except Exception as e:
            print(f"     ⚠️ 無法取得欄位類型資訊: {e}")
            return {}

    def _batch_update_records(self, update_df, table_name):
        """批次更新：itertuples + 共用 connection"""
        if update_df.empty:
            return
            
        # 更新所有欄位（除了主鍵）
        exclude_cols = ['created', 'tbiaID']
        update_cols = [col for col in update_df.columns if col not in exclude_cols]
        
        if not update_cols:
            return
        
        print(f"   🔄 批次更新 {len(update_df)} 筆記錄...")
        
        # 動態獲取欄位類型
        column_types = self._get_column_types(table_name)
        if not column_types:
            print(f"     ⚠️ 無法取得 {table_name} 的欄位類型，回退到逐筆更新")
            self._fallback_single_updates(update_df, table_name, update_cols)
            return
        
        # 預先計算欄位索引（itertuples 用）
        col_indices = {col: i for i, col in enumerate(update_df.columns)}
        tbia_idx = col_indices['tbiaID']
        col_idx_list = [(col, col_indices[col]) for col in update_cols]

        # 預先建立 SET 子句（每批都一樣，不用重建）
        set_clauses = []
        for j, col in enumerate(update_cols, 1):
            col_type = column_types.get(col, 'text')
            if col_type == 'timestamp':
                set_clauses.append(f'"{col}" = v.col_{j}::timestamp')
            elif col_type == 'numeric':
                set_clauses.append(f'"{col}" = v.col_{j}::numeric')
            elif col_type == 'boolean':
                set_clauses.append(f'"{col}" = v.col_{j}::boolean')
            elif col_type == 'geometry':
                set_clauses.append(f'"{col}" = v.col_{j}::geometry')
            else:
                set_clauses.append(f'"{col}" = v.col_{j}')
        
        col_aliases = ['tbia_id'] + [f'col_{j}' for j in range(1, len(update_cols) + 1)]
        set_clause_str = ', '.join(set_clauses)
        col_alias_str = ', '.join(col_aliases)

        large_batch_size = min(1000, len(update_df))
        
        try:
            with self.db.connect() as conn:
                for i in range(0, len(update_df), large_batch_size):
                    batch = update_df.iloc[i:i+large_batch_size]
                    
                    # 建立 VALUES 子句（itertuples）
                    values_list = []
                    for row in batch.itertuples(index=False):
                        values = [f"'{row[tbia_idx]}'"]
                        
                        for col, idx in col_idx_list:
                            value = row[idx]
                            col_type = column_types.get(col, 'text')
                            
                            if pd.isna(value) or value is None:
                                values.append('NULL')
                            elif col_type == 'timestamp':
                                values.append(f"{_safe_dollar_quote(value)}::timestamp")
                            elif col_type == 'numeric':
                                if isinstance(value, (int, float)) and not pd.isna(value):
                                    values.append(str(value))
                                else:
                                    values.append('NULL')
                            elif col_type == 'boolean':
                                if isinstance(value, bool):
                                    values.append('TRUE' if value else 'FALSE')
                                elif str(value).lower() in ['true', '1', 'yes', 't']:
                                    values.append('TRUE')
                                elif str(value).lower() in ['false', '0', 'no', 'f']:
                                    values.append('FALSE')
                                else:
                                    values.append('NULL')
                            elif col_type == 'geometry':
                                if isinstance(value, str) and value.startswith('POINT'):
                                    values.append(f"{_safe_dollar_quote(value)}::geometry")
                                else:
                                    values.append(f"ST_GeomFromText({_safe_dollar_quote(value)})")
                            else:
                                values.append(_safe_dollar_quote(value))

                        values_list.append(f"({', '.join(values)})")
                    
                    values_clause = ',\n    '.join(values_list)
                    
                    batch_sql = f"""
                    UPDATE {table_name} 
                    SET {set_clause_str}
                    FROM (VALUES 
                        {values_clause}
                    ) AS v({col_alias_str})
                    WHERE {table_name}."tbiaID" = v.tbia_id;
                    """
                    
                    result = conn.exec_driver_sql(batch_sql)
                    print(f"     ✅ 批次 {i//large_batch_size + 1}: 更新了 {result.rowcount} 筆")
                
                conn.commit()
                    
        except Exception as e:
            print(f"     ❌ 批次更新失敗: {e}")
            # 如果批次失敗，回退到逐筆更新
            self._fallback_single_updates(update_df, table_name, update_cols)
    
    def _fallback_single_updates(self, batch_df, table_name, update_cols):
        """回退到逐筆更新（當批次更新失敗時）"""
        print(f"     🔄 回退到逐筆更新 {len(batch_df)} 筆...")
        
        for _, row in batch_df.iterrows():
            try:
                # 建立 SET 子句
                set_clause = ', '.join([f'"{col}" = :{col}' for col in update_cols])
                
                # 建立參數字典
                params = {col: row[col] for col in update_cols}
                params['tbiaID'] = row['tbiaID']
                
                # 執行參數化更新
                update_sql = f"""
                UPDATE {table_name} 
                SET {set_clause}
                WHERE "tbiaID" = :tbiaID
                """
                
                with self.db.connect() as conn:
                    conn.execute(text(update_sql), params)
                    conn.commit()
                    
            except Exception as e:
                print(f"     ❌ 單筆更新失敗 {row['tbiaID']}: {e}")
                failed = row.to_dict()
                failed['_error'] = str(e)
                failed['_table'] = table_name
                self.failed_records.append(failed)

    def export_failed_records(self, filepath='failed_records.csv'):
        """匯出失敗記錄到 CSV"""
        if self.failed_records:
            pd.DataFrame(self.failed_records).to_csv(filepath, index=False)
            print(f"📄 已匯出 {len(self.failed_records)} 筆失敗記錄到 {filepath}")
        else:
            print("✅ 沒有失敗記錄")


class OptimizedMatchLogProcessor:
    """最佳化的 MatchLog 處理器"""
    
    def __init__(self, db_engine, batch_size=300):
        self.db = db_engine
        self.batch_size = batch_size
        self.failed_match_logs = []
    
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
            
        print(f"🎯 Processing {len(match_log_df)} match_log records...")
        start_time = time.time()
        
        # 1. 使用已取得的existed_records判斷（避免重複查詢）
        if existed_records is not None and not existed_records.empty:
            existing_ids = set(existed_records['tbiaID'].tolist())
            print(f"   📋 Using existing records info for match_log: {len(existing_ids)} existed")
        else:
            existing_ids = set()
            print(f"   📋 No existing records provided - treating all match_log as new")
        
        # 2. 分離新增和更新
        new_match_log = match_log_df[~match_log_df['tbiaID'].isin(existing_ids)].copy()
        update_match_log = match_log_df[match_log_df['tbiaID'].isin(existing_ids)].copy()
        
        print(f"   📝 New match_log: {len(new_match_log)}")
        print(f"   🔄 Update match_log: {len(update_match_log)}")
        
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
        print(f"✅ Match_log processing completed: {rate:.0f} records/sec")
    
    def _batch_update_match_log(self, update_df):
        """match_log 批次更新，使用固定的欄位類型（相對單純）"""
        if update_df.empty:
            return
            
        # 更新所有欄位（除了主鍵）
        exclude_cols = ['created', 'tbiaID']
        update_cols = [col for col in update_df.columns if col not in exclude_cols]
        
        if not update_cols:
            return
        
        print(f"   🎯 批次更新 match_log {len(update_df)} 筆記錄...")
        
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
                        values.append(f"{_safe_dollar_quote(value)}::timestamp")
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
                        values.append(_safe_dollar_quote(value))

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
                    print(f"     ✅ match_log 批次 {i//large_batch_size + 1}: 更新了 {result.rowcount} 筆")
                    
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