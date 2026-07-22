#!/usr/bin/env python3
"""從 Solr 抓取「未比對到」的 docs，取出指定欄位的唯一組合並輸出成 CSV。

未比對條件：
  1. 無 taxonID
  2. match_higher_taxon=True 且 taxonID 有值

用法範例：
  python extract_unmatched.py \
      --output unmatched.csv
"""
import argparse
import csv
import datetime
import re
import sys

import requests

SOLR_URL = "http://localhost:8983/solr/tbia_records"

FIELDS = [
    "sourceScientificName",
    "sourceVernacularName",
    "sourceTaxonRank",
    "sourceFamily",
    "sourceOrder",
    "sourceClass",
]
# 需要清除 HTML tag 的欄位
HTML_FIELDS = {"sourceScientificName", "sourceVernacularName"}
HTML_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(value: str) -> str:
    return HTML_TAG_RE.sub("", value).strip()


def fetch_docs(match_value, rows):
    """用 cursorMark 分頁抓取符合條件的所有 docs。"""
    query = f"(-taxonID:*) OR (taxonID:* AND match_higher_taxon:{match_value})"
    url = SOLR_URL.rstrip("/") + "/select"
    cursor = "*"
    while True:
        params = {
            "q": query,
            "fl": ",".join(FIELDS),
            "rows": rows,
            "sort": "id asc",  # cursorMark 需搭配唯一鍵排序
            "cursorMark": cursor,
            "wt": "json",
        }
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        docs = data["response"]["docs"]
        for doc in docs:
            yield doc
        next_cursor = data.get("nextCursorMark")
        if not docs or next_cursor == cursor:
            break
        cursor = next_cursor


def main():
    parser = argparse.ArgumentParser(description="抓取 Solr 未比對資料的唯一欄位組合")
    parser.add_argument("--output",
                        default=f"/bucket/unmatched_{datetime.date.today():%Y%m%d}.csv",
                        help="輸出 CSV 路徑")
    parser.add_argument("--match-value", default="True",
                        help="match_higher_taxon 的比對值（字串，預設 True）")
    parser.add_argument("--rows", type=int, default=5000, help="每次抓取筆數")
    args = parser.parse_args()

    seen = set()
    unique_rows = []
    for doc in fetch_docs(args.match_value, args.rows):
        row = []
        for field in FIELDS:
            value = doc.get(field, "")
            if isinstance(value, list):  # 保險：若為多值取第一個
                value = value[0] if value else ""
            value = value or ""
            if field in HTML_FIELDS:
                value = strip_html(value)
            row.append(value)
        key = tuple(row)
        if key not in seen:
            seen.add(key)
            unique_rows.append(row)

    with open(args.output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(FIELDS)
        writer.writerows(unique_rows)

    print(f"共輸出 {len(unique_rows)} 筆唯一組合到 {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()