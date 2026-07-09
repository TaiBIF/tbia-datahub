#!/usr/bin/env python3
"""
依 group + update_version (+ 選用 dataGeneralizations) 刪除 Solr 資料。
流程：dry-run 確認筆數 → 抽樣檢視 → 備份(JSONL) → 二次確認 → 刪除 → commit。
python delete_solr_with_dry_run.py --group tbri --version 2 --rights-holder "台灣生物多樣性網絡 TBN" --no-backup --execute
"""

import argparse
import json
import sys
from datetime import datetime

import requests
from pathlib import Path

# ---- 設定 ----
SOLR_URL = "http://solr:8983/solr"
CORE = "tbia_records"
SAMPLE_FIELDS = "id,group,rightsHolder,update_version,dataGeneralizations,scientificName,datasetName"
CSV_EXPORT_DIR = "/solr/csvs/export"
SAMPLE_ROWS = 10
# --------------


def build_query(group: str, version=None, data_generalizations=None,
                rights_holder=None) -> str:
    parts = [f'group:"{group}"']
    if version is not None:
        # update_version 為 pdouble，用範圍比對避開浮點精度問題
        parts.append(f'update_version:[{version} TO {version}]')
    if rights_holder is not None:
        parts.append(f'rightsHolder:"{rights_holder}"')
    if data_generalizations is not None:
        parts.append(f'dataGeneralizations:{str(data_generalizations).lower()}')
    return " AND ".join(parts)


def count(q: str) -> int:
    r = requests.get(
        f"{SOLR_URL}/{CORE}/select",
        params={"q": q, "rows": 0, "wt": "json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["response"]["numFound"]


def sample(q: str):
    r = requests.get(
        f"{SOLR_URL}/{CORE}/select",
        params={"q": q, "rows": SAMPLE_ROWS, "fl": SAMPLE_FIELDS, "wt": "json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["response"]["docs"]


def backup(q: str, total: int, path: str):
    """用 cursorMark 全量撈出存成 JSONL（每行一筆，避免欄位不齊問題）。"""
    written = 0
    cursor = "*"
    with open(path, "w", encoding="utf-8") as f:
        while True:
            r = requests.get(
                f"{SOLR_URL}/{CORE}/select",
                params={
                    "q": q,
                    "rows": 1000,
                    "sort": "id asc",
                    "cursorMark": cursor,
                    "wt": "json",
                },
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            docs = data["response"]["docs"]
            for doc in docs:
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                written += 1
            next_cursor = data.get("nextCursorMark")
            if next_cursor == cursor or not docs:
                break
            cursor = next_cursor
            print(f"  備份中… {written}/{total}", end="\r")
    print(f"  已備份 {written} 筆 → {path}")


def _flat(v):
    if isinstance(v, list):
        return "|".join(str(x) for x in v)
    return v


def delete(q: str):
    r = requests.post(
        f"{SOLR_URL}/{CORE}/update",
        params={"commit": "true"},
        headers={"Content-Type": "text/xml"},
        data=f"<delete><query>{_xml_escape(q)}</query></delete>".encode("utf-8"),
        timeout=120,
    )
    r.raise_for_status()


def _xml_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def import_csv(group: str, csv_dir: str = CSV_EXPORT_DIR):
    """POST csv_dir 底下以 group 開頭的 CSV 到 tbia_records（依 id upsert）。"""
    csv_path = Path(csv_dir)
    if not csv_path.is_dir():
        print(f"  匯入來源不存在：{csv_dir}，跳過。")
        return

    files = sorted(
        p for p in csv_path.iterdir()
        if p.is_file() and p.suffix == ".csv" and p.name.startswith(group)
    )
    if not files:
        print(f"  找不到以 '{group}' 開頭的 CSV（{csv_dir}），跳過匯入。")
        return

    print(f"  找到 {len(files)} 個 CSV，開始匯入…")
    for path in files:
        with open(path, "rb") as f:
            r = requests.post(
                f"{SOLR_URL}/{CORE}/update",
                params={"commit": "false"},
                headers={"Content-Type": "application/csv; charset=utf-8"},
                data=f,
                timeout=300,
            )
        r.raise_for_status()
        print(f"    匯入 {path.name}")

    requests.get(
        f"{SOLR_URL}/{CORE}/update", params={"commit": "true"}, timeout=60
    ).raise_for_status()
    print(f"  匯入完成，共 {len(files)} 個檔案。")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", required=True)
    ap.add_argument("--version", type=float, help="update_version 條件（選用）")
    ap.add_argument("--rights-holder", help="rightsHolder 條件（選用）")
    ap.add_argument("--data-generalizations", choices=["true", "false"],
                    help="加上 dataGeneralizations 條件")
    ap.add_argument("--csv-dir", default=CSV_EXPORT_DIR,
                    help=f"匯入來源資料夾（預設 {CSV_EXPORT_DIR}）")
    ap.add_argument("--execute", action="store_true",
                    help="實際執行刪除；不加則僅 dry-run")
    ap.add_argument("--no-backup", action="store_true", help="刪除前不備份（不建議）")
    ap.add_argument("--no-import", action="store_true", help="刪除後不匯入 CSV")
    args = ap.parse_args()

    if args.version is None and args.rights_holder is None:
        ap.error("至少需指定 --version 或 --rights-holder 其中之一，避免誤刪整個 group。")

    dg = None if args.data_generalizations is None else (args.data_generalizations == "true")
    q = build_query(args.group, args.version, dg, args.rights_holder)
    print(f"Query: {q}\n")

    total = count(q)
    print(f"符合筆數：{total}")

    if total > 0:
        print(f"\n抽樣前 {SAMPLE_ROWS} 筆：")
        for doc in sample(q):
            print("  ", {k: _flat(v) for k, v in doc.items()})

    if not args.execute:
        print("\n[DRY-RUN] 未刪除任何資料。確認無誤後加上 --execute 執行。")
        return

    if total > 0:
        ans = input(f"\n即將刪除 {total} 筆，輸入 group 名稱再次確認: ")
        if ans.strip() != args.group:
            print("確認失敗，中止。")
            sys.exit(1)

        if not args.no_backup:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            tag = "_".join(str(x) for x in
                           [args.group, args.rights_holder, args.version] if x is not None)
            path = f"backup_{tag}_{ts}.jsonl"
            print(f"\n備份中 → {path}")
            backup(q, total, path)

        print("\n執行刪除…")
        delete(q)
        print(f"刪除完成。殘餘符合筆數：{count(q)}")
    else:
        print("沒有符合的刪除資料。")

    if not args.no_import:
        print("\n匯入 CSV…")
        import_csv(args.group, args.csv_dir)


if __name__ == "__main__":
    main()