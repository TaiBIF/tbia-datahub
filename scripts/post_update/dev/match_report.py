#!/usr/bin/env python3
"""彙整 match_log → 快照 JSON + 各類 CSV + 前後比較 + email PNG，輸出到共享 volume。

放在 datahub 專案，月更新後執行。分類在 SQL 端聚合，不把原始資料拉進 Python，
故單位再大（數千萬筆）也只回傳幾列。

每個單位輸出（/portal/media/match_report/<YYYY-MM>/<group>/）：
  snapshot.json          各分類筆數／學名數
  unmatched_partner.csv  給夥伴：可由夥伴修正的未對到學名（含說明、來源階層、格式提醒）
  for_taicol.csv         給 TaiCOL：尚未收錄 + 僅對到上階
  name_state.csv.gz      每個學名的分類與筆數（供下次學名層級比較）
  compare_category.csv   與前次快照的分類層級比較（有前次才產出）
  compare_names.csv      與前次快照的學名層級比較（前次有 name_state 才產出）
  meta.json              本次產出資訊：前次月份、是否比對邏輯變更、學名變化摘要（供 stat_match 匯入）
  email.png              給夥伴的摘要圖

用法（在 /code 下以模組方式執行，才找得到專案根目錄的 app）：
  python -m scripts.post_update.dev.match_report --year-month 2026-11 --group namr   # 只跑單一單位（驗證用）
  python -m scripts.post_update.dev.match_report --year-month 2026-11                # 全部單位
  python -m scripts.post_update.dev.match_report --year-month 2026-11 --no-delta     # 比對邏輯變更後的首次產出
"""
import argparse
import csv
import datetime
import gzip
import json
import re
import sys
from pathlib import Path

from sqlalchemy import text, bindparam
from app import engine  # datahub 既有的 DB 連線（同 tbn.py）

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
import requests

OUT_BASE = Path("/portal/media/match_report")  # web 端對應 /tbia-volumes/media/match_report

# TaiCOL 名錄索引 core（uniqueKey=id，含 scientificName/taxonID/common_name_c/各階_c）
# ← 請改成實際 core 名稱
TAXON_SOLR_URL = "http://solr:8983/solr/taxa"

# ── 分類設定 ────────────────────────────────────────────────
# 內部 key → (軸, 顯示名稱, 歸責)
# 歸責：partner=夥伴可修, ours=本會/TaiCOL, both=視情況, review=內部檢討, info=僅計數
CATEGORY_META = {
    "atrank":      ("matched",   "對到（來源階層）",             None),
    "higher":      ("matched",   "僅對到上階（較來源退階）",     "both"),
    "fuzzy":       ("unmatched", "疑似錯字／格式",               "partner"),
    "multiple":    ("unmatched", "多個候選無法判斷",             "partner"),
    "nonstandard": ("unmatched", "非正規學名（sp./cf. 等）",     "partner"),
    "noname":      ("unmatched", "無學名（資料缺漏）",           "partner"),
    "placeholder": ("unmatched", "暫定名（GTDB 等資料庫編號）",  "info"),
    "vetoed":      ("unmatched", "疑應可對到（比對保守否決）",   "review"),
    "none":        ("unmatched", "TaiCOL 尚未收錄",              "ours"),
}

# 給夥伴 CSV 的補充說明（PNG 標籤空間有限，詳細說明放這裡）
CATEGORY_NOTE = {
    "fuzzy":       "學名與 TaiCOL 收錄名稱相近但不完全相同，請確認是否有錯字或格式問題。",
    "multiple":    "此學名在 TaiCOL 對應到多個分類群（如同名異物），無法判斷是哪一個。"
                   "建議增加上階層資訊（如界、綱、目、科）以利比對。",
    "nonstandard": "學名含 sp.、cf. 等開放命名標記，僅能對到上階層；若已鑑定到種，請更新學名。",
}

# 夥伴可修/需知的未對到類別（none→TaiCOL、vetoed→內部檢討、noname/placeholder 僅計數不列名）
PARTNER_CATS = {"fuzzy", "multiple", "nonstandard"}

_STAGES = ",".join(f"stage_{i}" for i in range(1, 9))
# 開放命名標記：句點可有可無，但須獨立成詞（避免誤判 sp019039055、species 等）
_NONSTD_RE = r'(^|[[:space:]])(sp|spp|cf|aff|indet)\.?([[:space:]]|$)'
# 開放命名中「只鑑定到前一個名稱」的標記（cf./aff. 為種階層的不確定，不包含在內）
_OPEN_RANK_RE = r'(^|[[:space:]])(sp|spp|indet)\.?([[:space:]]|$)'
# GTDB 等暫定名：sp 後接 6 位以上數字
_PLACEHOLDER_RE = r'[[:space:]]sp[0-9]{6,}'

# 品質以「來源自己的階層」為基準：match_higher_taxon=True 才算較來源退階；
# 但 sp./spp./indet. 以屬名（stage 4）對到時，對到的即為來源鑑定的階層，算 atrank。
# 未對到時，stage_1~8 多個 issue code 並存的優先序：vetoed > fuzzy > multiple > none
CATEGORY_CASE = f"""
CASE
  WHEN "is_matched" THEN
    CASE
      WHEN "match_higher_taxon" AND "match_stage" = 4
           AND "sourceScientificName" ~* '{_OPEN_RANK_RE}' THEN 'atrank'
      WHEN "match_higher_taxon" THEN 'higher'
      ELSE 'atrank'
    END
  ELSE
    CASE
      WHEN "sourceScientificName" IS NULL
        OR btrim("sourceScientificName") = ''            THEN 'noname'
      WHEN "sourceScientificName" ~  '{_PLACEHOLDER_RE}' THEN 'placeholder'
      WHEN "sourceScientificName" ~* '{_NONSTD_RE}'      THEN 'nonstandard'
      WHEN 'vetoed'   IN ({_STAGES}) THEN 'vetoed'
      WHEN 'fuzzy'    IN ({_STAGES}) THEN 'fuzzy'
      WHEN 'multiple' IN ({_STAGES}) THEN 'multiple'
      ELSE 'none'
    END
END
"""


# ── 查詢 ────────────────────────────────────────────────────
def list_groups(conn):
    rows = conn.execute(text('SELECT DISTINCT "group" FROM match_log ORDER BY "group"'))
    return [r[0] for r in rows if r[0]]


def rights_holder_of(conn, group):
    r = conn.execute(text(
        'SELECT "rights_holder" FROM match_log WHERE "group" = :g LIMIT 1'), {"g": group}
    ).first()
    return r[0] if r else group


def aggregate(conn, group):
    """回傳 [(category_key, records, unique_names), ...]，SQL 端聚合。"""
    sql = text(f"""
        SELECT {CATEGORY_CASE} AS cat,
               COUNT(*) AS records,
               COUNT(DISTINCT "sourceScientificName") AS unique_names
        FROM match_log
        WHERE "group" = :g
        GROUP BY 1
    """)
    return [(r[0], r[1], r[2]) for r in conn.execute(sql, {"g": group})]


def unmatched_names(conn, group):
    """未對到的唯一學名清單（給 CSV），SQL 端去重。"""
    sql = text(f"""
        SELECT "sourceScientificName" AS name,
               {CATEGORY_CASE} AS cat,
               COUNT(*) AS records
        FROM match_log
        WHERE "group" = :g AND NOT "is_matched"
        GROUP BY "sourceScientificName", 2
        ORDER BY records DESC
    """)
    return list(conn.execute(sql, {"g": group}))


def higher_names(conn, group):
    """有對到但只退到上階的唯一學名，帶對到的 taxonID。
    排除 sp./spp./indet. 以屬名對到者（已算 atrank，與 CATEGORY_CASE 一致）。"""
    sql = text(f"""
        SELECT "sourceScientificName" AS name, "taxonID" AS taxon_id, COUNT(*) AS records
        FROM match_log
        WHERE "group" = :g AND "is_matched" AND "match_higher_taxon"
          AND NOT ("match_stage" = 4
                   AND COALESCE("sourceScientificName", '') ~* '{_OPEN_RANK_RE}')
        GROUP BY "sourceScientificName", "taxonID"
        ORDER BY records DESC
    """)
    return list(conn.execute(sql, {"g": group}))


_SRC_EMPTY = {"vern": "", "rank": "", "fam": "", "ord": "", "cls": "", "kin": ""}


def source_fields_of(conn, group, names):
    """從 records 為每個唯一學名取一筆代表的 source 欄位（中文名、階層、各上階）。
    只查未對到的列（taxonID 為空），一名一列。"""
    names = [n for n in dict.fromkeys(names) if n]
    if not names:
        return {}
    sql = text("""
        SELECT DISTINCT ON ("sourceScientificName")
               "sourceScientificName" AS name,
               "sourceVernacularName" AS vern,
               "sourceTaxonRank"      AS rank,
               "sourceFamily"         AS fam,
               "sourceOrder"          AS ord,
               "sourceClass"          AS cls,
               "sourceKingdom"        AS kin
        FROM records
        WHERE "group" = :g AND "taxonID" IS NULL
          AND "sourceScientificName" IN :names
        ORDER BY "sourceScientificName"
    """).bindparams(bindparam("names", expanding=True))
    return {r.name: {k: (getattr(r, k) or "") for k in _SRC_EMPTY}
            for r in conn.execute(sql, {"g": group, "names": names})}


def _first(v):
    if isinstance(v, list):
        return v[0] if v else ""
    return v or ""


def _solr_lookup(field, values, fl, batch=100):
    """以 field:(值...) 批次查 taxon core，回 {值: doc}；查不到或連不上回空。

    用 POST 送 query（form-encoded），不受 URL 長度限制；batch 也調小，
    避免一次 OR 太多讓 Solr / 前置 proxy 回 400。
    """
    out, values = {}, [v for v in dict.fromkeys(values) if v]
    url = TAXON_SOLR_URL.rstrip("/") + "/select"
    for i in range(0, len(values), batch):
        chunk = values[i:i + batch]
        clause = " OR ".join('"%s"' % str(v).replace('"', r'\"') for v in chunk)
        try:
            r = requests.post(url, data={
                "q": "*:*", "fq": f"{field}:({clause})",
                "fl": ",".join([field] + fl), "rows": len(chunk), "wt": "json",
            }, timeout=60)
            r.raise_for_status()
            for d in r.json()["response"]["docs"]:
                key = _first(d.get(field))
                if key:
                    out[key] = d
        except Exception as e:
            print(f"Solr enrich 失敗（{field}）：{e}", file=sys.stderr)
    return out


# ── 格式判斷（與 SQL 的 regex 一致）────────────────────────────
_NONSTD_PY = re.compile(r'(^|\s)(sp|spp|cf|aff|indet)\.?(\s|$)', re.IGNORECASE)
_PLACEHOLDER_PY = re.compile(r'\ssp\d{6,}')
_CJK = re.compile(r'[\u4e00-\u9fff]')
# 作者名/年份：括號、四位數年份、&、或結尾為縮寫（如 "Kuk."、"L."）
_AUTHOR = re.compile(r'\(|\b\d{4}\b|&|\s[A-Z][A-Za-z\-]*\.\s*$')


def _is_nonstandard(name):
    name = name or ""
    return bool(_NONSTD_PY.search(name) or _PLACEHOLDER_PY.search(name))


def _is_hybrid(name):
    return "\u00d7" in name or any(t in ("x", "X") for t in name.split())


def _format_hints(name, s):
    """回傳格式提醒字串（多項以「；」分隔）。僅供參考，不影響原因分類。"""
    name = name or ""
    hints = []
    if _AUTHOR.search(name):
        hints.append("學名含作者／年份")
    if _is_hybrid(name):
        hints.append("雜交名")
    if s.get("vern") and s["vern"].strip() == name.strip():
        hints.append("俗名欄填入學名")
    if s.get("fam") and _CJK.search(s["fam"]):
        hints.append("科名混入中文")
    if not any(s.get(k) for k in ("fam", "ord", "cls", "kin")):
        hints.append("無上階層資訊")
    return "；".join(hints)


# ── 產出 ────────────────────────────────────────────────────
def write_snapshot(agg, group, rights_holder, year_month, out_dir):
    rows = []
    for key, records, uniq in agg:
        axis, label, resp = CATEGORY_META.get(key, ("unmatched", key, None))
        rows.append({
            "group": group, "rights_holder": rights_holder, "year_month": year_month,
            "axis": axis, "category_key": key, "category": label, "responsibility": resp,
            "records": int(records), "unique_names": int(uniq),
        })
    (out_dir / "snapshot.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return rows


def write_csvs(conn, group, rights_holder, out_dir):
    """拆兩份：unmatched_partner.csv（夥伴可修）＋ for_taicol.csv（回報 TaiCOL）。
    兩份都附來源階層與格式提醒，方便判斷是否為格式問題。"""
    un = unmatched_names(conn, group)      # (name, catkey, records)
    hi = higher_names(conn, group)         # (name, taxon_id, records)
    label = {k: v[1] for k, v in CATEGORY_META.items()}

    src = source_fields_of(conn, group, [n for n, _, _ in un])
    by_tid = _solr_lookup("id", [r[1] for r in hi],  # taxa core 主鍵為 id，值即 taxonID
                          ["common_name_c", "family", "family_c"])

    src_head = ["中文名(來源)", "階層(來源)", "科(來源)", "目(來源)", "綱(來源)", "界(來源)"]
    src_row = lambda s: [s["vern"], s["rank"], s["fam"], s["ord"], s["cls"], s["kin"]]

    # 給夥伴：fuzzy / multiple / nonstandard，依影響筆數排序
    with open(out_dir / "unmatched_partner.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["學名", "原因分類", "說明", "影響筆數"] + src_head + ["格式提醒"])
        for name, cat, records in un:
            if cat in PARTNER_CATS:
                s = src.get(name, _SRC_EMPTY)
                w.writerow([name, label.get(cat, cat), CATEGORY_NOTE.get(cat, ""), records]
                           + src_row(s) + [_format_hints(name, s)])

    # 給 TaiCOL：未收錄（none）＋ 僅對到上階（higher）；排除非正規學名與暫定名
    with open(out_dir / "for_taicol.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["學名", "類別", "影響筆數", "對到的taxonID", "中文名", "科",
                    "階層(來源)", "界(來源)", "格式提醒", "rights_holder"])
        for name, cat, records in un:
            if cat == "none" and not _is_nonstandard(name):
                s = src.get(name, _SRC_EMPTY)
                w.writerow([name, "TaiCOL 尚未收錄", records, "", s["vern"], s["fam"],
                            s["rank"], s["kin"], _format_hints(name, s), rights_holder])
        for name, tid, records in hi:
            if _is_nonstandard(name):
                continue
            d = by_tid.get(tid, {})
            cn = _first(d.get("common_name_c"))
            fam = _first(d.get("family_c")) or _first(d.get("family"))
            w.writerow([name, "僅對到上階", records, tid, cn, fam,
                        "", "", "", rights_holder])


# ── 前後比較 ────────────────────────────────────────────────
def prev_dir_of(group, year_month):
    """找本次之前、最近一次有該單位快照的月份；回 (year_month, 目錄) 或 (None, None)。"""
    if not OUT_BASE.exists():
        return None, None
    cands = sorted(p.name for p in OUT_BASE.iterdir()
                   if re.fullmatch(r"\d{4}-\d{2}", p.name) and p.name < year_month
                   and (p / group / "snapshot.json").exists())
    return (cands[-1], OUT_BASE / cands[-1] / group) if cands else (None, None)


def name_states(conn, group):
    """每個學名的主要分類（筆數最多者）與總筆數：{name: (cat, records)}。"""
    sql = text(f"""
        SELECT "sourceScientificName" AS name, {CATEGORY_CASE} AS cat, COUNT(*) AS records
        FROM match_log WHERE "group" = :g
        GROUP BY 1, 2
    """)
    acc = {}   # name -> (主要分類, 主要分類筆數, 總筆數)
    for name, cat, records in conn.execute(sql, {"g": group}):
        name = name or ""
        prev = acc.get(name)
        total = records + (prev[2] if prev else 0)
        if prev is None or records > prev[1]:
            acc[name] = (cat, records, total)
        else:
            acc[name] = (prev[0], prev[1], total)
    return {n: (c, t) for n, (c, _, t) in acc.items()}


def write_name_state(states, out_dir):
    with gzip.open(out_dir / "name_state.csv.gz", "wt", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "cat", "records"])
        for n, (c, r) in states.items():
            w.writerow([n, c, r])


def read_name_state(d):
    p = d / "name_state.csv.gz"
    if not p.exists():
        return None
    with gzip.open(p, "rt", newline="", encoding="utf-8") as f:
        return {r["name"]: (r["cat"], int(r["records"])) for r in csv.DictReader(f)}


# 學名變化類型 → 摘要 key（meta.json / 網頁用）
COMPARE_KIND_KEY = {
    "變差（原本對到來源階層）": "worse",
    "新對到來源階層": "new_atrank",
    "原因改變": "reason_changed",
    "新增學名": "new_name",
}


def write_compare(snapshot, states, prev_ym, prev_dir, out_dir):
    """產出 compare_category.csv 與 compare_names.csv。
    回傳學名變化摘要 {key: 學名數}；前次無 name_state 時回 None。"""
    label = {k: v[1] for k, v in CATEGORY_META.items()}

    # (1) 分類層級
    prev_rows = json.loads((prev_dir / "snapshot.json").read_text(encoding="utf-8"))
    pv = {r["category"]: r for r in prev_rows}
    cv = {r["category"]: r for r in snapshot}
    order = [v[1] for v in CATEGORY_META.values()]
    cats = [c for c in order if c in pv or c in cv] + \
           [c for c in {**pv, **cv} if c not in order]   # 前次有、本次已改名的舊分類
    with open(out_dir / "compare_category.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["分類", f"前次筆數({prev_ym})", "本次筆數", "筆數差異",
                    f"前次學名數({prev_ym})", "本次學名數", "學名數差異"])
        for c in cats:
            p, n = pv.get(c, {}), cv.get(c, {})
            pr, nr = p.get("records", 0), n.get("records", 0)
            pu, nu = p.get("unique_names", 0), n.get("unique_names", 0)
            w.writerow([c, pr, nr, nr - pr, pu, nu, nu - pu])

    # (2) 學名層級（前次無 name_state 時略過）
    prev_states = read_name_state(prev_dir)
    if prev_states is None:
        return None
    rows = []
    for name, (cat, rec) in states.items():
        old = prev_states.get(name)
        if old is None:
            if cat != "atrank":                      # 新出現且未達來源階層才列
                rows.append((name, "新增學名", "", cat, 0, rec))
            continue
        ocat, orec = old
        if ocat == cat:
            continue
        if cat == "atrank":
            kind = "新對到來源階層"
        elif ocat == "atrank":
            kind = "變差（原本對到來源階層）"
        else:
            kind = "原因改變"
        rows.append((name, kind, ocat, cat, orec, rec))

    kind_order = {"變差（原本對到來源階層）": 0, "新對到來源階層": 1, "原因改變": 2, "新增學名": 3}
    rows.sort(key=lambda r: (kind_order[r[1]], -r[5]))
    with open(out_dir / "compare_names.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["學名", "變化類型", f"前次分類({prev_ym})", "本次分類",
                    "前次筆數", "本次筆數"])
        for name, kind, ocat, cat, orec, rec in rows:
            w.writerow([name, kind, label.get(ocat, ocat), label.get(cat, cat), orec, rec])

    summary = {k: 0 for k in COMPARE_KIND_KEY.values()}
    for r in rows:
        summary[COMPARE_KIND_KEY[r[1]]] += 1
    return summary


def write_meta(out_dir, group, year_month, prev_ym, logic_changed, compare_summary):
    """本次產出資訊，供 stat_match 匯入 MatchReport。"""
    meta = {
        "group": group, "year_month": year_month,
        "prev_year_month": prev_ym,          # 無前次為 None
        "logic_changed": logic_changed,      # True：比對邏輯變更，差值不具比較意義
        "compare": compare_summary,          # 學名變化摘要；無前次 name_state 為 None
    }
    (out_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


# ── PNG ─────────────────────────────────────────────────────
# 字型跟腳本放一起，用相對路徑載入（容器無 fontconfig 也能跑）
_FONT = str(Path(__file__).with_name("NotoSansCJK-Regular.ttc"))
SANS = fm.FontProperties(fname=_FONT)
BOLD = fm.FontProperties(fname=_FONT); BOLD.set_weight("bold")
PAPER, INK, MUTED, ACCENT, GREEN, GREY = \
    "#f6f8f5", "#191e1b", "#5f6a64", "#2f6b5e", "#3f8f5f", "#c2cbc4"

# 品質光譜（以來源階層為基準）由好到差
SPECTRUM = [
    ("對到（來源階層）", "對到來源階層", "#2f6b5e"),
    ("僅對到上階（較來源退階）", "較來源退階", "#a7c4b7"),
    ("__unmatched__", "未對到", "#c2cbc4"),
]


def _atrank_rate(rows):
    total = sum(r["records"] for r in rows) or 1
    at = sum(r["records"] for r in rows
             if r["axis"] == "matched" and r["category"] == "對到（來源階層）")
    return round(at / total * 100, 1)


def render_png(snapshot_rows, rights_holder, year_month, prev_atrank_rate, prev_ym, out_dir):
    total = sum(r["records"] for r in snapshot_rows) or 1
    matched = sum(r["records"] for r in snapshot_rows if r["axis"] == "matched")
    overall = round(matched / total * 100, 1)
    prec = {r["category"]: r["records"] for r in snapshot_rows if r["axis"] == "matched"}
    unmatched_total = total - matched
    atrank_rate = _atrank_rate(snapshot_rows)
    delta = None if prev_atrank_rate is None else round(atrank_rate - prev_atrank_rate, 1)

    reasons = sorted(
        ((r["category"], r["records"], r["responsibility"])
         for r in snapshot_rows if r["axis"] == "unmatched"),
        key=lambda x: -x[1])[:3]

    fig = plt.figure(figsize=(6.4, 4.7), dpi=200)
    fig.patch.set_facecolor(PAPER)
    ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off")
    ax.set_xlim(0, 100); ax.set_ylim(0, 100)

    # 標頭
    ax.text(5, 93.5, rights_holder, fontproperties=BOLD, fontsize=13, color=INK)
    ax.text(5, 89, f"學名比對狀況 · {year_month}", fontproperties=SANS, fontsize=9, color=MUTED)

    # 主數字：對到來源階層（以來源自己的階層為基準，非「種」）
    ax.text(5, 74, f"{atrank_rate:.1f}%", fontproperties=BOLD, fontsize=38, color=INK)
    ax.text(5, 67.5, "對到來源提供的階層", fontproperties=SANS, fontsize=9.5, color=MUTED)
    ax.text(42, 77, f"整體對到 {overall:.1f}%", fontproperties=SANS, fontsize=9, color=MUTED)
    if delta is not None:
        arrow, col = ("▲", GREEN) if delta >= 0 else ("▼", "#c0533f")
        ax.text(42, 71, f"{arrow} {delta:+.1f}% 較前次（{prev_ym}）",
                fontproperties=BOLD, fontsize=10.5, color=col)

    # 品質光譜（堆疊長條）
    ax.text(5, 58, "資料解析品質 · 以來源階層為基準", fontproperties=SANS, fontsize=9, color=INK)
    seg = []
    for cat, short, color in SPECTRUM:
        val = unmatched_total if cat == "__unmatched__" else prec.get(cat, 0)
        if val > 0:
            seg.append((short, val, color))
    x, bw, y0, bh = 5, 90, 48, 6
    for short, val, color in seg:
        w = val / total * bw
        ax.add_patch(plt.Rectangle((x, y0), w, bh, color=color))
        x += w
    # 光譜圖例
    lx = 5
    for short, val, color in seg:
        pct = val / total * 100
        ax.add_patch(plt.Rectangle((lx, 41), 2, 2, color=color))
        t = f"{short} {pct:.1f}%"
        ax.text(lx + 2.8, 42, t, fontproperties=SANS, fontsize=8, color=MUTED, va="center")
        lx += 3.5 + len(t) * 1.75

    # 下半：有未對到才顯示原因；否則給一句品質建議
    ax.plot([5, 95], [35, 35], color="#dfe4de", lw=1)
    if reasons:
        ax.text(5, 30, "未對到學名 · 依原因分類", fontproperties=SANS, fontsize=9, color=INK)
        maxc = max(c for _, c, _ in reasons)
        for (label, cnt, resp), y in zip(reasons, [22, 15, 8]):
            color = ACCENT if resp == "partner" else GREY
            ax.text(30, y + 2.4, label, fontproperties=SANS, fontsize=8.5,
                    color=INK, ha="right", va="center")
            w = (cnt / maxc) * 42
            ax.add_patch(plt.Rectangle((32, y), w, 4.8, color=color))
            ax.text(33.2 + w, y + 2.4, f"{cnt:,}", fontproperties=SANS,
                    fontsize=8, color=MUTED, va="center")
        uncat = next((r["records"] for r in snapshot_rows
                      if r["category"] == "TaiCOL 尚未收錄"), 0)
        if uncat:
            ax.text(5, 2.5, f"{uncat:,} 筆為 TaiCOL 尚未收錄，已由本會彙整回報，無需貴單位處理。",
                    fontproperties=SANS, fontsize=7.6, color=MUTED)
    else:
        higher = prec.get("僅對到上階（較來源退階）", 0)
        hpct = higher / total * 100
        if higher:
            ax.text(5, 27, f"本次資料全數對到 TaiCOL，其中 {hpct:.1f}% 較來源退階（僅對到上階）。",
                    fontproperties=SANS, fontsize=9, color=INK)
            ax.text(5, 21, "多因 TaiCOL 尚未收錄該物種、或來源缺上階資訊；名單詳見附件。",
                    fontproperties=SANS, fontsize=9, color=MUTED)
        else:
            ax.text(5, 24, "本次資料全數對到來源提供的階層，品質良好。",
                    fontproperties=SANS, fontsize=9.5, color=INK)
    fig.savefig(out_dir / "email.png", facecolor=PAPER)
    plt.close(fig)


# ── 主流程 ──────────────────────────────────────────────────
def process_group(conn, group, year_month, show_delta=True):
    rh = rights_holder_of(conn, group)
    out_dir = OUT_BASE / year_month / group
    out_dir.mkdir(parents=True, exist_ok=True)

    agg = aggregate(conn, group)
    snapshot = write_snapshot(agg, group, rh, year_month, out_dir)
    write_csvs(conn, group, rh, out_dir)

    states = name_states(conn, group)
    write_name_state(states, out_dir)

    prev_ym, prev_dir = prev_dir_of(group, year_month)
    prev_rate, compare_summary = None, None
    if prev_dir:
        compare_summary = write_compare(snapshot, states, prev_ym, prev_dir, out_dir)
        if show_delta:
            prev_rate = _atrank_rate(json.loads(
                (prev_dir / "snapshot.json").read_text(encoding="utf-8")))
    write_meta(out_dir, group, year_month, prev_ym, not show_delta, compare_summary)
    render_png(snapshot, rh, year_month, prev_rate, prev_ym, out_dir)

    total = sum(r["records"] for r in snapshot)
    matched = sum(r["records"] for r in snapshot if r["axis"] == "matched")
    rate = round(matched / total * 100, 1) if total else 0.0
    print(f"[{group}] {rh}｜比對率 {rate}%｜{total} 筆｜前次 {prev_ym or '無'} → {out_dir}",
          file=sys.stderr)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year-month", default=f"{datetime.date.today():%Y-%m}",
                   help="YYYY-MM，預設本月")
    p.add_argument("--group", help="只處理單一單位（省略則全部）")
    p.add_argument("--no-delta", action="store_true",
                   help="比對邏輯變更後的首次產出：PNG 不顯示差值，meta.json 標記 logic_changed，"
                        "網頁同樣隱藏差值；比較檔仍會產出")
    args = p.parse_args()

    with engine.connect() as conn:
        groups = [args.group] if args.group else list_groups(conn)
        for g in groups:
            process_group(conn, g, args.year_month, show_delta=not args.no_delta)
    print(f"完成 {len(groups)} 個單位", file=sys.stderr)


if __name__ == "__main__":
    main()