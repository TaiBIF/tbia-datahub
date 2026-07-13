#!/usr/bin/env python3
"""
匯入 [group]_[info_id]_*.csv 到 Solr 再刪除該 rightsHolder 的舊版本殘餘資料

流程：
  1. 取得新版本號（--version, 未指定則由 postgres update_version 表自動取得
     該 rights_holder 最新且 is_finished=true 的 update_version)
  2. dry-run: 顯示刪除條件、符合筆數、抽樣
  3. 二次確認
  4. 平行匯入 CSV (commit=false 最後統一 openSearcher=true)
  5. 刪除 group + rightsHolder + update_version < 新版本 的殘餘資料

先匯入再刪除，避免刪除到匯入完成之間出現「某單位資料全部消失」的空窗

使用範例
python scripts/post_update/solr/post_and_delete.py \
    --group nps --info-id 0 --rights-holder "臺灣國家公園生物多樣性資料庫"

python scripts/post_update/solr/post_and_delete.py \
    --group nmns --info-id 0 --rights-holder "科博典藏 (NMNS Collection)" \
    --version 14 --workers 8 --execute --yes
"""

import argparse
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from app import db_settings
import psycopg2
import requests

# ---- 設定 ----
SOLR_URL = os.getenv("SOLR_URL", "http://solr:8983/solr")
CORE = "tbia_records"
SAMPLE_FIELDS = "id,group,rightsHolder,update_version,dataGeneralizations,scientificName,datasetName"
CSV_EXPORT_DIR = "/solr/csvs/export"
SAMPLE_ROWS = 10
DEFAULT_WORKERS = 6
# --------------

_local = threading.local()


def get_session() -> requests.Session:
    """每個 thread 一個 Session，重用 TCP 連線。"""
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.mount("http://", requests.adapters.HTTPAdapter(
            pool_connections=16, pool_maxsize=16, max_retries=0))
        _local.session = s
    return _local.session


# ---------- postgres ----------

def fetch_latest_version(rights_holder: str) -> int:
    """取得該 rights_holder 最新且 is_finished=true 的 update_version。"""
    conn = psycopg2.connect(**db_settings)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT update_version FROM update_version "
                "WHERE rights_holder = %s AND is_finished IS TRUE "
                "AND update_version IS NOT NULL "
                "ORDER BY update_version DESC LIMIT 1;",
                (rights_holder,),
            )
            row = cursor.fetchone()
    finally:
        conn.close()

    if not row:
        raise SystemExit(
            f"postgres update_version 表中找不到 rights_holder='{rights_holder}' "
            f"且 is_finished=true 的紀錄，請確認或改用 --version 手動指定。"
        )
    return int(row[0])


# ---------- solr query ----------

def build_delete_query(group: str, rights_holder: str, new_version: int,
                       data_generalizations=None) -> str:
    """刪除條件：同 group + rightsHolder，且版本早於本次新版本。"""
    parts = [
        f'group:"{group}"',
        f'rightsHolder:"{rights_holder}"',
        # update_version 為 pdouble，上界開區間排除新版本本身
        f'update_version:[* TO {new_version}}}',
    ]
    if data_generalizations is not None:
        parts.append(f'dataGeneralizations:{str(data_generalizations).lower()}')
    return " AND ".join(parts)


def count(q: str) -> int:
    r = get_session().get(
        f"{SOLR_URL}/{CORE}/select",
        params={"q": q, "rows": 0, "wt": "json"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["response"]["numFound"]


def sample(q: str):
    r = get_session().get(
        f"{SOLR_URL}/{CORE}/select",
        params={"q": q, "rows": SAMPLE_ROWS, "fl": SAMPLE_FIELDS, "wt": "json"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["response"]["docs"]


def _flat(v):
    if isinstance(v, list):
        return "|".join(str(x) for x in v)
    return v


def _xml_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def delete(q: str):
    r = get_session().post(
        f"{SOLR_URL}/{CORE}/update",
        params={"commit": "true"},
        headers={"Content-Type": "text/xml"},
        data=f"<delete><query>{_xml_escape(q)}</query></delete>".encode("utf-8"),
        timeout=1800,
    )
    r.raise_for_status()


# ---------- csv import ----------

def list_csv_files(group: str, info_id: str, csv_dir: str):
    csv_path = Path(csv_dir)
    if not csv_path.is_dir():
        raise SystemExit(f"匯入來源資料夾不存在：{csv_dir}")

    prefix = f"{group}_{info_id}_"
    files = sorted(
        p for p in csv_path.iterdir()
        if p.is_file() and p.suffix == ".csv" and p.name.startswith(prefix)
    )
    if not files:
        raise SystemExit(f"找不到符合 '{prefix}*.csv' 的檔案（{csv_dir}）")
    return files


def _post_one(path: Path):
    """單檔 POST commit 交給 solrconfig 的 autoCommit (openSearcher=false) 處理"""
    with open(path, "rb") as f:
        r = get_session().post(
            f"{SOLR_URL}/{CORE}/update",
            params={"commit": "false"},
            headers={"Content-Type": "application/csv; charset=utf-8"},
            data=f,
            timeout=900,
        )
    r.raise_for_status()
    return path


def final_commit():
    """唯一一次開新 searcher 的 commit 讓所有新資料一次生效"""
    r = get_session().get(
        f"{SOLR_URL}/{CORE}/update",
        params={"commit": "true", "openSearcher": "true", "softCommit": "false"},
        timeout=1800,
    )
    r.raise_for_status()


def import_csv(files, workers: int) -> list:
    """平行匯入，回傳失敗的 (檔名, 錯誤) 清單。"""
    total = len(files)
    print(f"  共 {total} 個 CSV，使用 {workers} 條平行連線匯入…")

    failures = []
    done = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_post_one, p): p for p in files}
        for fut in as_completed(futures):
            path = futures[fut]
            try:
                fut.result()
            except Exception as e:
                failures.append((path.name, repr(e)))
                print(f"    [失敗] {path.name}: {e}")
            with lock:
                done += 1
                if done % 50 == 0 or done == total:
                    print(f"    進度 {done}/{total}（失敗 {len(failures)}）")

    print("  最終 commit (openSearcher=true) …")
    final_commit()
    return failures


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", required=True)
    ap.add_argument("--info-id", required=True,
                    help="info_id, 決定 CSV 檔名前綴 [group]_[info_id]_*.csv")
    ap.add_argument("--rights-holder", required=True,
                    help="rightsHolder, 同時用於查詢 postgres update_version")
    ap.add_argument("--version", type=int,
                    help="本次匯入 CSV 所帶的『新』版本號；"
                         "未指定則自動取 postgres 中該 rights_holder "
                         "最新且 is_finished=true 的 update_version")
    ap.add_argument("--data-generalizations", choices=["true", "false"],
                    help="刪除條件額外加上 dataGeneralizations")
    ap.add_argument("--csv-dir", default=CSV_EXPORT_DIR,
                    help=f"匯入來源資料夾（預設 {CSV_EXPORT_DIR})")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"平行匯入連線數（預設 {DEFAULT_WORKERS})")
    ap.add_argument("--execute", action="store_true",
                    help="實際執行匯入與刪除；不加則僅 dry-run")
    ap.add_argument("--no-delete", action="store_true",
                    help="只匯入，不刪除舊版本")
    ap.add_argument("--yes", action="store_true",
                    help="略過二次確認（背景執行時使用）")
    args = ap.parse_args()

    # 1) 版本號
    if args.version is not None:
        new_version = args.version
        print(f"新版本（手動指定）：{new_version}")
    else:
        new_version = fetch_latest_version(args.rights_holder)
        print(f"新版本(postgres 自動取得, is_finished=true):{new_version}")

    # 2) CSV
    files = list_csv_files(args.group, args.info_id, args.csv_dir)
    print(f"待匯入 CSV:{len(files)} 個（{args.csv_dir}/{args.group}_{args.info_id}_*.csv）")

    # 3) 刪除條件
    dg = None if args.data_generalizations is None else (args.data_generalizations == "true")
    q = build_delete_query(args.group, args.rights_holder, new_version, dg)
    print(f"\n刪除條件(匯入後才執行):\n  {q}")

    total = count(q)
    print(f"\n目前符合刪除條件筆數:{total}")
    print("（匯入後這個數字會下降，實際刪除的是仍未被更新到新版本的殘餘資料）")

    if total > 0:
        print(f"\n抽樣前 {SAMPLE_ROWS} 筆：")
        for doc in sample(q):
            print("  ", {k: _flat(v) for k, v in doc.items()})

    if not args.execute:
        print("\n[DRY-RUN] 未匯入也未刪除。確認無誤後加上 --execute 執行。")
        return

    # 4) 二次確認
    if not args.yes:
        ans = input(
            f"\n即將匯入 {len(files)} 個 CSV，再刪除舊版本殘餘資料。"
            f"\n輸入 group 名稱確認: "
        )
        if ans.strip() != args.group:
            print("確認失敗，中止。")
            sys.exit(1)

    # 5) 匯入
    print("\n[1/2] 匯入 CSV…")
    failures = import_csv(files, args.workers)

    if failures:
        print(f"\n匯入有 {len(failures)} 個檔案失敗：")
        for name, err in failures:
            print(f"  - {name}: {err}")
        print("\n為避免資料遺失，已中止刪除步驟。請修正後重跑（重跑會依 id upsert，不會重複）。")
        sys.exit(1)

    print(f"  匯入完成，共 {len(files)} 個檔案。")

    if args.no_delete:
        print("\n[--no-delete] 略過刪除步驟。")
        return

    # 6) 刪除舊版本殘餘
    remain = count(q)
    print(f"\n[2/2] 匯入後仍符合舊版條件（= 來源已下架 / 未更新）筆數：{remain}")

    if remain == 0:
        print("沒有需要刪除的舊資料，完成。")
        return

    print("執行刪除…")
    delete(q)
    print(f"刪除完成。殘餘符合筆數：{count(q)}")


if __name__ == "__main__":
    main()