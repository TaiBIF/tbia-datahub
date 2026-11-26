"""
v1 core（tbia_records）全部資料 → v1 格式 CSV（一次性工具，2.6 的來源；上線後隨 scripts/v2_migration/ 刪除）

11/26：rematch 最後一次只重跑部分學名，/solr/csvs/export 只剩部分 rematch_*.csv、archive 不是最新，
不能直接當 2.6 來源；改從 v1 core 匯出（等於線上完整內容，所有 rematch 結果都已匯入）。

- cursorMark 依 id 排序分頁讀取，每 FILE_ROWS 筆寫一個 CSV（v1_dump_00001.csv …）到 --out
- 欄位值還原成原本 v1 CSV 的寫法：日期 2026-11-23T12:18:00Z → 2026-11-23 12:18:00、布林 True / False、
  多值欄位（schemaless 建成 multiValued）只有一個值時取該值，多個值以 | 合併並計數
- 每寫完一個檔就存進度（_state.json），中斷後加 --resume 從下一個檔接續
- 結束時比對匯出筆數與 core 筆數

用法（測試站 ~/tbia-datahub-v2，背景執行）：
  docker compose -f docker-compose.v2.yml exec -d app-v2 sh -c \
    'python -u -m scripts.v2_migration.dump_v1_core --out /bucket/v2/v1_dump > /bucket/v2/v1_dump.log 2>&1'
  # 中斷後接續
  ... python -u -m scripts.v2_migration.dump_v1_core --out /bucket/v2/v1_dump --resume >> /bucket/v2/v1_dump.log 2>&1
接著 2.6：convert_v1_csv --src /bucket/v2/v1_dump --all ...（見交接文件）
"""
import argparse
import csv
import glob
import json
import os
import re
import sys
import time
from collections import Counter

import requests

from scripts.utils.export import SOLR_URL

FILE_ROWS = 50000
PAGE_ROWS = 10000   # FILE_ROWS 須為 PAGE_ROWS 的倍數：每個檔剛好寫完時 buf 為空，才能存可接續的 cursor
SKIP_FIELDS = {'_version_', '_root_', '_nest_path_', '_text_'}
ISO_RE = re.compile(r'^(\d{4}-\d\d-\d\d)T(\d\d:\d\d:\d\d)(?:\.\d+)?Z$')
STATE = '_state.json'


def norm(v, multi_counter, field):
    if isinstance(v, list):
        if len(v) == 1:
            v = v[0]
        else:
            multi_counter[field] += 1
            v = '|'.join(str(norm(x, multi_counter, field)) for x in v)
    if v is None:
        return ''
    if isinstance(v, bool):
        return 'True' if v else 'False'
    if isinstance(v, str):
        m = ISO_RE.match(v)
        return f'{m.group(1)} {m.group(2)}' if m else v
    return v


def fetch(core, cursor, retries=5):
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(f'{SOLR_URL}/{core}/select', timeout=300, json={
                'query': '*:*', 'limit': PAGE_ROWS, 'sort': 'id asc', 'fields': '*',
                'params': {'cursorMark': cursor}})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f'[dump] 讀取失敗（第 {attempt}/{retries} 次）：{e}', flush=True)
            time.sleep(10 * attempt)
    raise RuntimeError('Solr 讀取重試用盡，已存的檔案保留，修復後加 --resume 接續')


def write_file(path, docs, multi_counter):
    cols = []
    seen = set()
    for d in docs:
        for k in d:
            if k not in seen and k not in SKIP_FIELDS:
                seen.add(k)
                cols.append(k)
    tmp = path + '.tmp'
    with open(tmp, 'w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for d in docs:
            w.writerow([norm(d.get(c), multi_counter, c) for c in cols])
    os.replace(tmp, path)       # 寫完才改名，中斷時不會留下半個檔


def main(argv=None):
    p = argparse.ArgumentParser(description='v1 core → v1 格式 CSV（2.6 來源）')
    p.add_argument('--core', default='tbia_records')
    p.add_argument('--out', required=True)
    p.add_argument('--resume', action='store_true')
    a = p.parse_args(argv)

    if os.path.abspath(a.out).startswith('/solr/csvs/export'):
        sys.exit('--out 不可放在 /solr/csvs/export（那是 v1 更新流程的目錄）')
    assert FILE_ROWS % PAGE_ROWS == 0
    os.makedirs(a.out, exist_ok=True)
    state_path = os.path.join(a.out, STATE)

    total = requests.get(f'{SOLR_URL}/{a.core}/select', params={'q': '*:*', 'rows': 0}, timeout=60).json()['response']['numFound']
    if a.resume and os.path.exists(state_path):
        st = json.load(open(state_path))
        cursor, file_no, written = st['cursor'], st['file_no'], st['written']
        print(f'[dump] 接續：第 {file_no + 1} 個檔起，已匯出 {written:,} 筆', flush=True)
    else:
        if glob.glob(os.path.join(a.out, 'v1_dump_*.csv')):
            sys.exit(f'{a.out} 已有 v1_dump_*.csv；要接續請加 --resume，要重來請先清空目錄')
        cursor, file_no, written = '*', 0, 0
    print(f'[dump] {a.core} 共 {total:,} 筆 → {a.out}（每檔 {FILE_ROWS:,} 筆）', flush=True)

    multi = Counter()
    buf = []
    start = time.time()
    while True:
        j = fetch(a.core, cursor)
        docs = j['response']['docs']
        nxt = j['nextCursorMark']
        buf += docs
        done = nxt == cursor
        while len(buf) >= FILE_ROWS or (done and buf):
            chunk, buf = buf[:FILE_ROWS], buf[FILE_ROWS:]
            file_no += 1
            write_file(os.path.join(a.out, f'v1_dump_{file_no:05d}.csv'), chunk, multi)
            written += len(chunk)
            # 進度只在檔案寫完、且 buf 已清空時存（cursor 對應到 buf 為空的位置），接續時不會漏或重複
            if not buf:
                json.dump({'cursor': nxt, 'file_no': file_no, 'written': written}, open(state_path, 'w'))
            if file_no % 20 == 0:
                rate = written / max(time.time() - start, 1)
                print(f'[dump] {file_no} 檔、{written:,} / {total:,} 筆，約 {rate:,.0f} 筆/秒', flush=True)
        if done:
            break
        cursor = nxt

    print(f'[dump] 完成：{file_no} 檔、{written:,} 筆；core {total:,} 筆', flush=True)
    if multi:
        print(f'[dump] 多值欄位（以 | 合併）：{dict(multi)}', flush=True)
    if written != total:
        print(f'[dump] ⚠️ 筆數不符：匯出 {written:,} / core {total:,}（匯出期間 core 有變動？）', flush=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
