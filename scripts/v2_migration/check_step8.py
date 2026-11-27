"""
步驟 8（8.0 schemaless、8.1 正式切換）Solr / datahub 端檢查。只讀（schemaless 階段只寫暫存 core）。
每項印 PASS / FAIL / SKIP / INFO，編號對應交接文件「8.0 / 8.1 測試清單」。

不 import datahub 的程式（pre / build 階段在正式站跑時，datahub 還是 main），只用 requests。
切換前從釋出目錄執行；切換後（datahub 已部署 v2）也可在 /code 執行：

  # 切換前（兩站相同；釋出目錄由 git archive 產生，見 8.1 ①）
  docker exec -i tbia-datahub-app-container python /bucket/v2/release/scripts/v2_migration/check_step8.py <階段> [選項]
  # 切換後
  docker exec -i -w /code tbia-datahub-app-container python scripts/v2_migration/check_step8.py post

階段：
  schemaless  8.0：對暫存 core（名稱須以 schemaless_test 開頭）驗證未定義欄位回 400、日期與數字仍可寫入
  pre         8.1 ① 前：core 狀態、v1 筆數、/bucket/v2/export、partner-info API、磁碟；寫出 / 比對兩站指紋
  build       8.1 ① 後：tbia_records_build（v2）與 tbia_records（v1）筆數、各 group / 來源資料庫一致
  post        8.1 ⑥：切換後 tbia_records（v2）、tbia_records_v1、新 build core、tw_grid、datahub 設定
  rollback    回滾後：tbia_records 回到 v1
"""
import argparse
import csv
import glob
import hashlib
import json
import os
import random
import shutil
import sys
import traceback
import xml.etree.ElementTree as ET
from fnmatch import fnmatch

import requests

SOLR = os.getenv('SOLR_URL', 'http://solr:8983/solr').rstrip('/')
PROD_PARTNER_API = 'https://tbiadata.tw/api/partner-info'
TIMEOUT = 300

# 與 scripts/utils/export.py 的 SOURCE_DATABASE_MAP 相同（v1 rightsHolder 舊名稱 → v2 sourceDatabase）
SOURCE_DATABASE_MAP = {
    '中央研究院生物多樣性中心動物標本館': '中央研究院動物標本館 ASIZ',
    '中央研究院生物多樣性中心植物標本資料庫': '中央研究院植物標本館 HAST',
    '國家海洋資料庫及共享平台': '國家海洋資料庫及共享平臺',
    '國立海洋生物博物館生物典藏管理系統': '生物典藏管理系統',
    '科博典藏 (NMNS Collection)': '科博典藏',
    '國立臺灣博物館典藏': '國立臺灣博物館生物典藏資料',
    '愛自然-臺灣(iNaturalist Taiwan)': '愛自然-臺灣',
    '中油生態地圖': '中油生態保育資料庫',
}

RESULTS = []


def report(no, ok, msg=''):
    status = {True: 'PASS', False: 'FAIL', None: 'SKIP', 'info': 'INFO'}[ok]
    RESULTS.append((no, status))
    print(f'[{status}] {no} {msg}', flush=True)


def run(no, fn, *args):
    try:
        fn(*args)
    except Exception as e:
        report(no, False, f'例外：{type(e).__name__}: {e}')
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Solr 小工具
# ---------------------------------------------------------------------------

def cores_status():
    r = requests.get(f'{SOLR}/admin/cores', params={'action': 'STATUS', 'wt': 'json'}, timeout=TIMEOUT)
    r.raise_for_status()
    out = {}
    for name, s in r.json()['status'].items():
        idx = s.get('index', {})
        out[name] = {'instanceDir': s.get('instanceDir'), 'numDocs': idx.get('numDocs'),
                     'size': idx.get('size'), 'sizeInBytes': idx.get('sizeInBytes')}
    return out


def fields(core):
    r = requests.get(f'{SOLR}/{core}/schema/fields', params={'wt': 'json'}, timeout=TIMEOUT)
    r.raise_for_status()
    return {f['name'] for f in r.json()['fields']}


def count(core, q='*:*', fq=None):
    params = {'q': q, 'rows': 0, 'wt': 'json'}
    if fq:
        params['fq'] = fq
    r = requests.get(f'{SOLR}/{core}/select', params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()['response']['numFound']


def facet(core, field, fq=None):
    r = requests.get(f'{SOLR}/{core}/select', timeout=TIMEOUT, params={
        'q': '*:*', 'rows': 0, 'wt': 'json', 'facet': 'true', 'facet.field': field, 'fq': fq or [],
        'facet.limit': -1, 'facet.mincount': 1, 'facet.missing': 'true'})
    r.raise_for_status()
    f = r.json()['facet_counts']['facet_fields'][field]
    return {('(空)' if f[i] is None else f[i]): f[i + 1] for i in range(0, len(f), 2) if f[i + 1]}


def schema_kind(core):
    """'v1'（rightsHolder、沒有 tbiaPartnerID）、'v2'（sourceDatabase + tbiaPartnerID、沒有 rightsHolder）或 '?'"""
    fs = fields(core)
    if 'rightsHolder' in fs and 'tbiaPartnerID' not in fs:
        return 'v1'
    if {'sourceDatabase', 'tbiaPartnerID'} <= fs and 'rightsHolder' not in fs:
        return 'v2'
    return '?'


def schemaless_off(core):
    """solrconfig 的 add-unknown-fields-to-the-schema chain 不含 add-schema-fields（8.0）"""
    r = requests.get(f'{SOLR}/{core}/admin/file', params={'file': 'solrconfig.xml', 'contentType': 'text/xml'},
                     timeout=TIMEOUT)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    for chain in root.iter('updateRequestProcessorChain'):
        if chain.get('name') == 'add-unknown-fields-to-the-schema':
            return 'add-schema-fields' not in (chain.get('processor') or '')
    return True   # 沒有這條 chain 也不會自動建欄位


def post_csv(core, text, commit=False):
    return requests.post(f'{SOLR}/{core}/update', params={'commit': 'true' if commit else 'false'},
                         data=text.encode('utf-8'), headers={'Content-Type': 'text/csv; charset=utf-8'},
                         timeout=TIMEOUT)


def diff_dict(a, b, limit=10):
    keys = sorted(set(a) | set(b), key=str)
    bad = [(k, a.get(k, 0), b.get(k, 0)) for k in keys if a.get(k, 0) != b.get(k, 0)]
    return bad[:limit], len(bad)


def mapped_rights_holder(core):
    """v1 core 的 rightsHolder facet 換成 v2 名稱（同名合併）"""
    out = {}
    for k, v in facet(core, 'rightsHolder').items():
        nk = SOURCE_DATABASE_MAP.get(k, k)
        out[nk] = out.get(nk, 0) + v
    return out


def need_core(st, name):
    if name not in st:
        raise RuntimeError(f'找不到 core {name}')


# ---------------------------------------------------------------------------
# 8.0 schemaless
# ---------------------------------------------------------------------------

def phase_schemaless(a):
    core = a.core
    if not core.startswith('schemaless_test'):
        sys.exit('schemaless 階段只對名稱以 schemaless_test 開頭的暫存 core 寫入')
    if count(core) != 0:
        sys.exit(f'{core} 不是空的，請用新建的暫存 core')
    before = fields(core)

    def s0():
        report('8.0-0', schemaless_off(core), 'solrconfig 的 chain 已拿掉 add-schema-fields')

    def s1():
        r = post_csv(core, 'id,zz_v2_unknown_field\nv2-s1,abc\n', commit=True)
        ok = r.status_code == 400 and 'unknown field' in r.text.lower()
        report('8.0-1', ok, f'未定義欄位：HTTP {r.status_code}（應為 400 unknown field）{r.text[:150] if not ok else ""}')

    def s2():
        r = post_csv(core, 'id,created,year,decimalLatitude,sourceDatabase,group\n'
                           'v2-s2,2024-01-02 03:04:05,2024,23.5,測試資料庫,test\n', commit=True)
        if r.status_code != 200:
            report('8.0-2', False, f'HTTP {r.status_code} {r.text[:200]}')
            return
        d = requests.get(f'{SOLR}/{core}/select', params={'q': 'id:v2-s2', 'wt': 'json'},
                         timeout=TIMEOUT).json()['response']['docs'][0]
        ok = (d.get('created') == '2024-01-02T03:04:05Z' and d.get('year') == 2024
              and d.get('decimalLatitude') == 23.5 and d.get('sourceDatabase') == '測試資料庫')
        report('8.0-2', ok, f"'YYYY-MM-DD HH:MM:SS' 日期、整數、小數可寫入且為單值：{d}")

    def s3():
        r = post_csv(core, 'id,year,decimalLatitude\nv2-s3,,\n', commit=True)
        n = count(core, 'id:v2-s3')
        report('8.0-3', r.status_code == 200 and n == 1, f'空字串欄位：HTTP {r.status_code}、寫入 {n} 筆（應為 200、1）')

    def s4():
        added = fields(core) - before
        report('8.0-4', not added, f'schema 沒有自動新增欄位{"：" + ", ".join(sorted(added)) if added else ""}')

    for no, fn in [('8.0-0', s0), ('8.0-1', s1), ('8.0-2', s2), ('8.0-3', s3), ('8.0-4', s4)]:
        run(no, fn)
    requests.post(f'{SOLR}/{core}/update', params={'commit': 'true'}, json={'delete': {'query': '*:*'}},
                  timeout=TIMEOUT)
    print(f'（已清空 {core}；接著照文件 UNLOAD 刪除暫存 core）')


# ---------------------------------------------------------------------------
# 8.1 ① 前
# ---------------------------------------------------------------------------

def schema_field_patterns(conf_dir):
    root = ET.parse(os.path.join(conf_dir, 'managed-schema')).getroot()
    names = {f.get('name') for f in root.iter('field')}
    patterns = [f.get('name') for f in root.iter('dynamicField')]
    return names, patterns


def export_stats(export_dir):
    files = sorted(glob.glob(os.path.join(export_dir, '*.csv')))
    sizes = [(os.path.basename(f), os.path.getsize(f)) for f in files]
    digest = hashlib.md5(json.dumps(sizes).encode()).hexdigest()
    return files, {'files': len(files), 'bytes': sum(s for _, s in sizes), 'name_size_md5': digest}


def phase_pre(a):
    st = cores_status()
    for name in sorted(st):
        s = st[name]
        print(f"  core {name}: instanceDir={s['instanceDir']} numDocs={s['numDocs']} size={s['size']}")
    fp = {}

    def p1():
        missing = [c for c in ('tbia_records', 'tbia_records_build', 'tw_grid') if c not in st]
        exists = [c for c in ('tbia_records_v1', 'tw_grid_next') if c in st]
        report('8.1-P1', not missing and not exists,
               f'需要的 core 都在、tbia_records_v1 / tw_grid_next 尚未存在（缺 {missing or "無"}；已存在 {exists or "無"}）')

    def p2():
        k = schema_kind('tbia_records')
        report('8.1-P2', k == 'v1', f'tbia_records 為 v1 schema（目前 {k}）')

    def p3():
        total = count('tbia_records')
        fp['total'] = total
        fp['group'] = facet('tbia_records', 'group')
        fp['rightsHolder'] = facet('tbia_records', 'rightsHolder')
        if a.expect_total is None:
            report('8.1-P3', 'info', f'tbia_records {total:,} 筆（未給 --expect-total）')
        else:
            report('8.1-P3', total == a.expect_total, f'tbia_records {total:,} 筆（應為 {a.expect_total:,}）')

    def p4():
        if not os.path.isdir(a.export_dir):
            report('8.1-P4', False, f'找不到 {a.export_dir}')
            return
        files, stats = export_stats(a.export_dir)
        fp['export'] = stats
        names, patterns = schema_field_patterns(a.conf)
        bad, no_id = {}, []
        for f in files:
            with open(f, encoding='utf-8-sig', newline='') as fh:
                header = next(csv.reader(fh), [])
            if 'id' not in header:
                no_id.append(os.path.basename(f))
            for c in header:
                if c not in names and not any(fnmatch(c, p) for p in patterns):
                    bad.setdefault(c, os.path.basename(f))
        ok = bool(files) and not bad and not no_id
        report('8.1-P4', ok, f"{a.export_dir}：{stats['files']:,} 檔、{stats['bytes'] / 1e9:,.1f} GB；"
                             f"v2 schema 沒有的欄位 {bad or '無'}；缺 id 欄 {no_id[:5] or '無'}")

    def p5():
        url = os.getenv('PORTAL_PARTNER_API') or PROD_PARTNER_API
        if url != PROD_PARTNER_API:
            report('8.1-P5', False, f'PORTAL_PARTNER_API={url}：上線應不設或設 {PROD_PARTNER_API}（上線 .env 檢查表）')
            return
        r = requests.get(url, timeout=60)
        data = r.json().get('data', []) if r.status_code == 200 else []
        api_groups = {d['group'] for d in data}
        solr_groups = set(fp.get('group') or facet('tbia_records', 'group')) - {'gbif', '(空)'}
        missing = sorted(solr_groups - api_groups)
        no_id = sorted(d['group'] for d in data if d['group'] != 'gbif' and not d.get('tbiaPartnerID'))
        report('8.1-P5', r.status_code == 200 and not missing and not no_id,
               f'{url} HTTP {r.status_code}，{len(data)} 個 group；資料中有但 API 沒有的 group {missing or "無"}；'
               f'沒有 tbiaPartnerID {no_id or "無"}')

    def p6():
        if not os.path.isdir('/solr'):
            report('8.1-P6', None, '容器內沒有 /solr（Solr 資料目錄），請在主機 df -h 確認')
            return
        need = (st.get('tbia_records', {}).get('sizeInBytes') or 0) * 1.2
        free = shutil.disk_usage('/solr').free
        report('8.1-P6', free > need, f'Solr 磁碟剩 {free / 1e9:,.0f} GB，灌入 v2 約需 {need / 1e9:,.0f} GB（tbia_records × 1.2）')

    for no, fn in [('8.1-P1', p1), ('8.1-P2', p2), ('8.1-P3', p3), ('8.1-P4', p4), ('8.1-P5', p5), ('8.1-P6', p6)]:
        run(no, fn)

    if a.fingerprint_out:
        with open(a.fingerprint_out, 'w', encoding='utf-8') as fh:
            json.dump(fp, fh, ensure_ascii=False, indent=1, sort_keys=True)
        print(f'（指紋寫到 {a.fingerprint_out}）')
    if a.compare:
        with open(a.compare, encoding='utf-8') as fh:
            other = json.load(fh)
        for no, key in [('8.1-P7a', 'total'), ('8.1-P7b', 'group'), ('8.1-P7c', 'rightsHolder'), ('8.1-P7d', 'export')]:
            same = other.get(key) == fp.get(key)
            detail = '' if same else f'：本站 {str(fp.get(key))[:300]}；{a.compare} {str(other.get(key))[:300]}'
            report(no, same, f'{key} 與 {os.path.basename(a.compare)} 相同{detail}')


# ---------------------------------------------------------------------------
# 8.1 ① 後
# ---------------------------------------------------------------------------

def phase_build(a):
    build, live = a.build_core, 'tbia_records'
    st = cores_status()
    need_core(st, build)
    totals = {}

    def b1():
        k = schema_kind(build)
        off = schemaless_off(build)
        report('8.1-B1', k == 'v2' and off, f'{build} 為 v2 schema（{k}）、schemaless 已關（{off}）')

    def b2():
        totals['build'], totals['live'] = count(build), count(live)
        exp = '' if a.expect_total is None else f'，應為 {a.expect_total:,}'
        ok = totals['build'] == totals['live'] and (a.expect_total is None or totals['build'] == a.expect_total)
        report('8.1-B2', ok, f"{build} {totals['build']:,} 筆、{live} {totals['live']:,} 筆{exp}")

    def b3():
        bad, n = diff_dict(facet(live, 'group'), facet(build, 'group'))
        report('8.1-B3', n == 0, f'各 group 筆數一致（不同 {n} 個：{bad}）')

    def b4():
        v2 = facet(build, 'sourceDatabase')
        bad, n = diff_dict(mapped_rights_holder(live), v2)
        old = {k: v2[k] for k in SOURCE_DATABASE_MAP if k in v2}
        report('8.1-B4', n == 0 and not old, f'各來源資料庫筆數一致（不同 {n} 個：{bad}）；舊名稱殘留 {old or "無"}')

    def b5():
        has = count(build, 'tbiaPartnerID:*')
        gbif = count(build, 'group:gbif')
        total = totals.get('build') or count(build)
        report('8.1-B5', has == total - gbif, f'tbiaPartnerID 有值 {has:,}，應為全部 − GBIF = {total - gbif:,}')

    def b6():
        if not os.path.exists(a.fail_log):
            report('8.1-B6', None, f'找不到 {a.fail_log}（import 時有設 FAIL_LOG 嗎？）')
            return
        lines = [x for x in open(a.fail_log, encoding='utf-8').read().splitlines() if x.strip()]
        report('8.1-B6', not lines, f'匯入失敗 {len(lines)} 檔 {lines[:5]}')

    def b7():
        seed = random.randint(1, 10 ** 6)
        r = requests.get(f'{SOLR}/{build}/select', timeout=TIMEOUT, params={
            'q': '*:*', 'rows': a.sample, 'fl': 'id', 'sort': f'random_{seed} asc', 'wt': 'json'})
        r.raise_for_status()
        ids = [d['id'] for d in r.json()['response']['docs']]
        r = requests.post(f'{SOLR}/{live}/select', timeout=TIMEOUT, json={
            'query': '*:*', 'limit': len(ids), 'fields': 'id',
            'filter': '{!terms f=id}' + ','.join(ids)})
        r.raise_for_status()
        found = {d['id'] for d in r.json()['response']['docs']}
        missing = [i for i in ids if i not in found]
        report('8.1-B7', not missing, f'隨機抽 {len(ids)} 筆 id 在 {live} 都找得到（找不到 {missing[:5] or "無"}）')

    for no, fn in [('8.1-B1', b1), ('8.1-B2', b2), ('8.1-B3', b3), ('8.1-B4', b4), ('8.1-B5', b5),
                   ('8.1-B6', b6), ('8.1-B7', b7)]:
        run(no, fn)


# ---------------------------------------------------------------------------
# 8.1 ⑥ 切換後 / 回滾後
# ---------------------------------------------------------------------------

def phase_post(a):
    st = cores_status()
    for name in sorted(st):
        s = st[name]
        print(f"  core {name}: instanceDir={s['instanceDir']} numDocs={s['numDocs']} size={s['size']}")
    totals = {}

    def q1():
        for c in ('tbia_records', 'tbia_records_v1', 'tbia_records_build', 'tw_grid'):
            need_core(st, c)
        kinds = {c: schema_kind(c) for c in ('tbia_records', 'tbia_records_v1', 'tbia_records_build')}
        offs = {c: schemaless_off(c) for c in ('tbia_records', 'tbia_records_build')}
        ok = kinds == {'tbia_records': 'v2', 'tbia_records_v1': 'v1', 'tbia_records_build': 'v2'} and all(offs.values())
        report('8.1-Q1', ok, f'schema：{kinds}；schemaless 已關：{offs}')

    def q2():
        totals['v2'], totals['v1'], totals['build'] = (count('tbia_records'), count('tbia_records_v1'),
                                                       count('tbia_records_build'))
        ok = totals['v2'] == totals['v1'] and totals['build'] == 0 and (
            a.expect_total is None or totals['v2'] == a.expect_total)
        report('8.1-Q2', ok, f"tbia_records {totals['v2']:,}、tbia_records_v1 {totals['v1']:,}"
                             f"{'' if a.expect_total is None else f'（應為 {a.expect_total:,}）'}、"
                             f"tbia_records_build {totals['build']:,}（應為 0）")

    def q3():
        bad, n = diff_dict(mapped_rights_holder('tbia_records_v1'), facet('tbia_records', 'sourceDatabase'))
        report('8.1-Q3', n == 0, f'tbia_records 各來源資料庫 = tbia_records_v1 換新名稱後（不同 {n} 個：{bad}）')

    def q4():
        fs = fields('tw_grid')
        n = count('tw_grid')
        names = set(facet('tw_grid', 'sourceDatabase')) if 'sourceDatabase' in fs else set()
        old = sorted(names & set(SOURCE_DATABASE_MAP))
        v2_names = set(facet('tbia_records', 'sourceDatabase'))
        extra = sorted(names - v2_names - {'(空)'})
        report('8.1-Q4', 'sourceDatabase' in fs and n > 0 and not old and not extra,
               f'tw_grid 為 v2（sourceDatabase 欄位 {"有" if "sourceDatabase" in fs else "沒有"}）、{n:,} 筆、'
               f'舊名稱 {old or "無"}、tbia_records 沒有的名稱 {extra or "無"}')
        if 'tw_grid_next' in st:
            report('8.1-Q4b', 'info', f"tw_grid_next（回滾用的舊 tw_grid）{st['tw_grid_next']['numDocs']:,} 筆，觀察期後刪除")

    def q5():
        bad = {k: os.getenv(k) for k, default in [('RECORDS_CORE', 'tbia_records'), ('TW_GRID_CORE', 'tw_grid'),
                                                    ('LOCALITY_CORE', 'locality'), ('DATASET_CORE', 'dataset')]
               if os.getenv(k) not in (None, '', default)}
        api = os.getenv('PORTAL_PARTNER_API')
        if api not in (None, '', PROD_PARTNER_API):
            bad['PORTAL_PARTNER_API'] = api
        report('8.1-Q5', not bad, f'datahub .env 沒有開發期的值（有問題：{bad or "無"}）')
        tv = os.getenv('TAXA_VERSION')
        report('8.1-Q5b', bool(tv) or None, f'TAXA_VERSION={tv or "（未設，nameAccordingTo 會是空的）"}')

    def q6():
        code = '/code'
        cfg = os.path.join(code, 'solr-workspace/conf-tbia/tbia_records/solrconfig.xml')
        dd = os.path.join(code, 'scripts/utils/deduplicates.py')
        if not os.path.exists(dd):
            report('8.1-Q6', None, '找不到 /code（不在 datahub app 容器？）')
            return
        deployed = 'sourceDatabase:' in open(dd, encoding='utf-8').read()
        conf_ok = True
        for chain in ET.parse(cfg).getroot().iter('updateRequestProcessorChain'):
            if chain.get('name') == 'add-unknown-fields-to-the-schema':
                conf_ok = 'add-schema-fields' not in (chain.get('processor') or '')
        report('8.1-Q6', deployed and conf_ok, f'datahub 已部署 v2：deduplicates 查 sourceDatabase（{deployed}）、'
                                               f'repo solrconfig 已關 schemaless（{conf_ok}）')

    def q7():
        if not os.path.exists('/code/scripts/utils/deduplicates.py'):
            report('8.1-Q7', None, '找不到 /code')
            return
        sys.path.insert(0, '/code')
        from scripts.utils.deduplicates import get_existed_records_optimized   # 切換後才有 v2 版
        rh = facet('tbia_records_v1', 'rightsHolder', fq='occurrenceID:*')
        name = min((k for k in rh if k != '(空)'), key=lambda k: rh[k])   # 有 occurrenceID、筆數最少的單位
        r = requests.get(f'{SOLR}/tbia_records_v1/select', timeout=TIMEOUT, params={
            'q': '*:*', 'fq': [f'rightsHolder:"{name}"', 'occurrenceID:*'], 'rows': 20, 'fl': 'id,occurrenceID',
            'wt': 'json'})
        docs = [d for d in r.json()['response']['docs'] if ',' not in d['occurrenceID']]
        occ = [d['occurrenceID'] for d in docs]
        df = get_existed_records_optimized(occ, name)   # update 腳本傳入的是舊名稱
        got = set(df['tbiaID']) if len(df) else set()
        want = {d['id'] for d in docs}
        report('8.1-Q7', bool(want) and want <= got,
               f'deduplicates 以舊名稱「{name}」取回既有 tbiaID：{len(want & got)} / {len(want)} 筆（應全部取回）')

    for no, fn in [('8.1-Q1', q1), ('8.1-Q2', q2), ('8.1-Q3', q3), ('8.1-Q4', q4), ('8.1-Q5', q5),
                   ('8.1-Q6', q6), ('8.1-Q7', q7)]:
        run(no, fn)


def phase_rollback(a):
    def r1():
        k = schema_kind('tbia_records')
        n = count('tbia_records')
        exp = '' if a.expect_total is None else f'（應為 {a.expect_total:,}）'
        report('8.1-R1', k == 'v1' and (a.expect_total is None or n == a.expect_total),
               f'tbia_records 回到 v1 schema（{k}）、{n:,} 筆{exp}')

    def r2():
        fs = fields('tw_grid')
        report('8.1-R2', 'sourceDatabase' not in fs or 'rightsHolder' in fs, 'tw_grid 回到 v1 conf')
    for no, fn in [('8.1-R1', r1), ('8.1-R2', r2)]:
        run(no, fn)


def main(argv=None):
    p = argparse.ArgumentParser(description='步驟 8 Solr / datahub 檢查')
    p.add_argument('phase', choices=['schemaless', 'pre', 'build', 'post', 'rollback'])
    p.add_argument('--core', default='schemaless_test', help='schemaless 階段的暫存 core')
    p.add_argument('--build-core', default='tbia_records_build')
    p.add_argument('--expect-total', type=int, help='v1 筆數（例 35051922）')
    p.add_argument('--export-dir', default='/bucket/v2/export')
    p.add_argument('--conf', default='/bucket/v2/release/solr-workspace/conf-tbia/tbia_records',
                   help='v2 managed-schema 所在目錄（檢查 CSV 表頭）')
    p.add_argument('--fail-log', default='/bucket/v2/import_failures.txt')
    p.add_argument('--sample', type=int, default=200)
    p.add_argument('--fingerprint-out', help='pre：把筆數與 CSV 指紋寫到這個檔案')
    p.add_argument('--compare', help='pre：與另一站的指紋檔比對')
    a = p.parse_args(argv)
    print(f'Solr: {SOLR}　階段: {a.phase}')
    {'schemaless': phase_schemaless, 'pre': phase_pre, 'build': phase_build,
     'post': phase_post, 'rollback': phase_rollback}[a.phase](a)
    n = {s: sum(1 for _, x in RESULTS if x == s) for s in ('PASS', 'FAIL', 'SKIP', 'INFO')}
    print(f"合計：PASS {n['PASS']}、FAIL {n['FAIL']}、SKIP {n['SKIP']}、INFO {n['INFO']}")
    sys.exit(1 if n['FAIL'] else 0)


if __name__ == '__main__':
    main()
