import time
import pandas as pd
import numpy as np
from sqlalchemy import text, inspect
from app import engine
from app import SessionLocal
from scripts.utils.progress import timed

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

    # 1. 統一替換「偽空值」與「空字串」
    null_indicators = ['NA', '-99999', '-999999', -99999, -999999, 'N/A', 'nan', '', 'None', 'NaT']
    df = df.replace(null_indicators, np.nan)

    datetime_cols = ['created', 'modified', 'standardDate', 'sourceCreated', 'sourceModified']

    for col in datetime_cols:
        if col in df.columns:
            # 1. 【關鍵防護】無論它現在是什麼型態，一律強制轉回標準 Pandas 時間格式
            # errors='coerce' 會把那些根本不是日期的亂碼直接變成 NaT，確保後續不會噴錯
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # 2. 安全地使用 .dt 將時間轉為標準格式字串
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            # 3. 將字串化後產生的 'NaT', 'NaN' 與底層 np.nan 徹底替換為 None
            df[col] = df[col].replace({'NaT': None, 'NaN': None, np.nan: None})

    # 2. 基礎欄位設定與改名
    df['is_deleted'] = False
    df['update_version'] = int(update_version)
    df = df.rename(columns={'id': 'tbiaID'})
    
    # 3. 欄位過濾：只保留資料表 (records_cols) 裡面有的欄位
    # (假設 records_cols 是全域變數或有從外部傳入)
    columns_to_keep = df.columns.intersection(records_cols)
    df = df[columns_to_keep]

    # 4. 字串處理與換行符號清洗
    for col in df.columns:
        # 只針對 object 型態 (現在包含了字串與剛剛轉換好的日期) 處理 \r 問題
        if df[col].dtype == object:
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].apply(
                lambda x: x.replace('\r\n', '\n').replace('\r', '\n') if isinstance(x, str) else x
            )

    # 5. 終極空值轉換 (這步最關鍵)
    # 剛剛日期欄位的 NaT 已經被處理成 None 了
    # 這裡的 where 會負責把其他文字或數值欄位裡的 np.nan 也全部轉為 None
    df = df.where(pd.notna(df), None)

    return df

def delete_records(rights_holder, group, update_version, exclude_ids=None):
    """
    刪除 update_version 不一致的 records 與對應 match_log。

    Args:
        exclude_ids: 要保留、不予刪除的 tbiaID 集合（例如本輪 upsert 失敗的 records，
                     需要保留舊版資料）。為 None 或空時走原本的簡單 SQL。
                     對於大量 ID（例如 10 萬筆以上）會自動走 temp table + COPY，
                     避免巨大的 array 參數拖垮 query plan。
    """
    import io

    exclude_ids = [str(x) for x in exclude_ids] if exclude_ids else []
    params = {
        'update_version': update_version,
        'rights_holder': rights_holder,
        'group': group,
    }

    with engine.connect() as conn:
        if exclude_ids:
            # 建立 temp table 並用 COPY 灌入失敗的 tbiaID
            conn.exec_driver_sql(
                'CREATE TEMP TABLE _exclude_ids (tbia_id TEXT PRIMARY KEY) ON COMMIT DROP'
            )

            buffer = io.StringIO()
            for tid in exclude_ids:
                buffer.write(f"{tid}\n")
            buffer.seek(0)

            raw_conn = conn.connection
            cur = raw_conn.cursor()
            cur.copy_expert("COPY _exclude_ids (tbia_id) FROM STDIN", buffer)

            query = text("""
                WITH deleted_ids AS (
                    DELETE FROM records r
                    WHERE update_version != :update_version
                      AND "rightsHolder" = :rights_holder
                      AND "group" = :group
                      AND NOT EXISTS (
                          SELECT 1 FROM _exclude_ids e WHERE e.tbia_id = r."tbiaID"
                      )
                    RETURNING "tbiaID"
                )
                DELETE FROM match_log
                WHERE "tbiaID" IN (SELECT "tbiaID" FROM deleted_ids);
            """)
        else:
            query = text("""
                WITH deleted_ids AS (
                    DELETE FROM records
                    WHERE update_version != :update_version
                      AND "rightsHolder" = :rights_holder
                      AND "group" = :group
                    RETURNING "tbiaID"
                )
                DELETE FROM match_log
                WHERE "tbiaID" IN (SELECT "tbiaID" FROM deleted_ids);
            """)

        conn.execute(query, params)
        conn.commit()


class OptimizedRecordsProcessor:
    """最佳化的 Records 處理器"""
    
    def __init__(self, db_engine, batch_size=200):
        self.db = db_engine
        self.batch_size = batch_size
        self.failed_records = []
        self.success_count = 0
        self._column_types_cache = {}

    def _safe_dollar_quote(self, value):
        """安全的 PostgreSQL dollar quoting，處理 $v$、\0"""
        s = str(value)
        s = s.replace('\0', '')           # 移除 null byte
        s = s.replace('$v$', '$v$$v$')    # 跳脫 dollar quote tag
        return f"$v${s}$v$"

    @timed()
    def smart_upsert_records(self, df, existed_records=None, table_name='records', dedup_tracker=None):
        """
        智能 UPSERT：使用已取得的existed_records，避免重複查詢

        Args:
            df: 要處理的資料
            existed_records: 已存在的記錄(從get_existed_records_optimized取得)
            table_name: 目標表名
            dedup_tracker: 若提供，本次成功寫入的 records 會自動透過
                           record_batch_keys 註冊到 SQLite，供同 run 跨批次 dedup 使用

        Returns:
            set: 本次寫入失敗的 tbiaID 集合（空集合代表全部成功）
        """
        if df.empty:
            return set()

        # print(f"🔄 Processing {len(df)} records with smart upsert...")
        start_time = time.time()

        # 1. 使用已取得的existed_records（避免重複查詢）
        if existed_records is not None and not existed_records.empty:
            existing_ids = set(existed_records['tbiaID'].tolist())
            # print(f"   📋 Using existing records info: {len(existing_ids)} existed")
        else:
            existing_ids = set()
            # print(f"   📋 No existing records provided - treating all as new")

        # 2. 分離新增和更新資料
        new_records = df[~df['tbiaID'].isin(existing_ids)].copy()
        update_records = df[df['tbiaID'].isin(existing_ids)].copy()

        # print(f"   📝 New records: {len(new_records)}")
        # print(f"   🔄 Update records: {len(update_records)}")

        failed_ids = set()

        # 3. 批次新增（使用 COPY）
        if not new_records.empty:
            insert_start = time.time()
            failed_ids |= self._copy_insert_records(new_records, table_name)
            # print(f"   ✅ Inserted {len(new_records)} records in {time.time() - insert_start:.2f}s")

        # 4. 批次更新
        if not update_records.empty:
            update_start = time.time()
            failed_ids |= self._batch_update_records(update_records, table_name)
            # print(f"   ✅ Updated {len(update_records)} records in {time.time() - update_start:.2f}s")

        # 5. 累積成功筆數 + 註冊 dedup keys（僅針對成功寫入的 records）
        batch_success = len(df) - len(failed_ids)
        self.success_count += batch_success
        if dedup_tracker is not None and batch_success > 0:
            successful_df = df[~df['tbiaID'].isin(failed_ids)] if failed_ids else df
            dedup_tracker.record_batch_keys(successful_df)

        total_time = time.time() - start_time
        rate = len(df) / total_time if total_time > 0 else 0
        # print(f"🎯 Smart upsert completed: {len(df)} records in {total_time:.2f}s ({rate:.0f} records/sec)")

        return failed_ids

    def _copy_insert_records(self, df, table_name):
        """使用 COPY 協議批次新增，比 to_sql 快 5-7 倍。COPY 失敗時逐筆 fallback。

        Returns:
            set: 本次新增失敗的 tbiaID 集合
        """
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
            return set()
        except Exception as e:
            raw_conn.rollback()
            # print(f"     ⚠️ COPY 失敗，回退到逐筆 insert: {e}")
            return self._fallback_single_inserts(df, table_name)
        finally:
            raw_conn.close()

    def _fallback_single_inserts(self, batch_df, table_name):
        """逐筆 INSERT fallback（COPY 失敗時使用），追蹤每一筆的成敗。

        Returns:
            set: 失敗的 tbiaID 集合
        """
        failed_ids = set()
        cols = list(batch_df.columns)
        col_list = ', '.join([f'"{c}"' for c in cols])
        placeholders = ', '.join([f':{c}' for c in cols])
        insert_sql = f'INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})'

        for _, row in batch_df.iterrows():
            try:
                params = {col: row[col] for col in cols}
                with self.db.connect() as conn:
                    conn.execute(text(insert_sql), params)
                    conn.commit()
            except Exception as e:
                # print(f"     ❌ 單筆新增失敗 {row['tbiaID']}: {e}")
                failed = row.to_dict()
                failed['_error'] = str(e)
                failed['_table'] = table_name
                failed['_op'] = 'insert'
                self.failed_records.append(failed)
                failed_ids.add(row['tbiaID'])

        return failed_ids

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
            # print(f"     ⚠️ 無法取得欄位類型資訊: {e}")
            return {}

    def _batch_update_records(self, update_df, table_name):
        """批次更新：itertuples + 共用 connection

        Returns:
            set: 失敗的 tbiaID 集合
        """
        if update_df.empty:
            return set()

        # 更新所有欄位（除了主鍵）
        exclude_cols = ['created', 'tbiaID']
        update_cols = [col for col in update_df.columns if col not in exclude_cols]

        if not update_cols:
            return set()

        # print(f"   🔄 批次更新 {len(update_df)} 筆記錄...")

        # 動態獲取欄位類型
        column_types = self._get_column_types(table_name)
        if not column_types:
            # print(f"     ⚠️ 無法取得 {table_name} 的欄位類型，回退到逐筆更新")
            return self._fallback_single_updates(update_df, table_name, update_cols)
        
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
                                values.append(f"{self._safe_dollar_quote(value)}::timestamp")
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
                                    values.append(f"{self._safe_dollar_quote(value)}::geometry")
                                else:
                                    values.append(f"ST_GeomFromText({self._safe_dollar_quote(value)})")
                            else:
                                values.append(self._safe_dollar_quote(value))

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
                    # print(f"     ✅ 批次 {i//large_batch_size + 1}: 更新了 {result.rowcount} 筆")
                
                conn.commit()
                return set()

        except Exception as e:
            # print(f"     ❌ 批次更新失敗: {e}")
            # 如果批次失敗，回退到逐筆更新
            return self._fallback_single_updates(update_df, table_name, update_cols)

    def _fallback_single_updates(self, batch_df, table_name, update_cols):
        """回退到逐筆更新（當批次更新失敗時）

        Returns:
            set: 失敗的 tbiaID 集合
        """
        # print(f"     🔄 回退到逐筆更新 {len(batch_df)} 筆...")

        failed_ids = set()
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
                # print(f"     ❌ 單筆更新失敗 {row['tbiaID']}: {e}")
                failed = row.to_dict()
                failed['_error'] = str(e)
                failed['_table'] = table_name
                failed['_op'] = 'update'
                self.failed_records.append(failed)
                failed_ids.add(row['tbiaID'])

        return failed_ids

    def export_failed_records(self, filepath='failed_records.csv'):
        """匯出失敗記錄到 CSV"""
        if self.failed_records:
            pd.DataFrame(self.failed_records).to_csv(filepath, index=False)
            print(f"📄 已匯出 {len(self.failed_records)} 筆失敗記錄到 {filepath}")
        else:
            print("✅ 沒有 records 失敗記錄")