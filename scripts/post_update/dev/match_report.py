#!/usr/bin/env python3
"""彙整 match_log → 快照 JSON + 各類 CSV + 前後比較 + email PNG，輸出到共享 volume。

放在 datahub 專案，月更新後執行。分類在 SQL 端聚合，不把原始資料拉進 Python。

效能：開始時只掃 match_log 一次、records 一次，彙總成兩張暫存表（_mr_agg、_mr_src），
各資料庫的報告都從暫存表產出，不需替 match_log / records 另建索引。

學名的判斷：依比對流程的順序（stage 1 → 3 → 5）取 sourceScientificName →
sourceVernacularName → originalVernacularName 第一個非空值；三者皆空才算「無學名」。

每個資料庫（單位）輸出（/portal/media/match_report/<YYYY-MM>/<group>_<info_id>/）：
  同一個 group 可能有多個資料庫（info_id 不同，如 nps_0 國家公園、nps_1 濕地），各自一份報告
  snapshot.json          各分類筆數／學名數
  unmatched_partner.csv  給夥伴：可由夥伴修正的學名（未對到各原因＋疑似錯字；含說明、來源階層、格式提醒）
  for_taicol.csv         給 TaiCOL：尚未收錄 + 僅對到上階 + 字尾變化 + 拼寫不同（已對到但拼寫與 TaiCOL 不同）
  noname_records.zip     給夥伴：無學名（學名、中文名、原始名稱皆空白）且未對到的逐筆紀錄（有才產出）
  name_state.csv.gz      每個學名的分類與筆數（供下次學名層級比較）
  compare_category.csv   與前次快照的分類層級比較（有前次才產出）
  compare_names.csv      與前次快照的學名層級比較（前次有 name_state 才產出）
  meta.json              本次產出資訊：前次月份、是否比對邏輯變更、學名變化摘要（供 stat_match 匯入）
  email.png              給夥伴的摘要圖

用法（在 /code 下以模組方式執行，才找得到專案根目錄的 app）：
  python -m scripts.post_update.dev.match_report --year-month 2026-11 --group nps namr       # 只跑這些 group 的資料庫
  python -m scripts.post_update.dev.match_report --year-month 2026-11 --unit nps_1 namr_0    # 只跑這些資料庫
  python -m scripts.post_update.dev.match_report --year-month 2026-11                 # 全部
  python -m scripts.post_update.dev.match_report --year-month 2026-11 --no-delta      # 比對邏輯變更後的首次產出
"""
import argparse
import csv
import datetime
import gzip
import io
import zipfile
import json
import re
import sys
from pathlib import Path

from sqlalchemy import text, bindparam
from app import engine  # datahub 既有的 DB 連線（同 tbn.py）
from scripts.utils.match import RANK_PREFIX_RE   # 與比對端共用「學名前誤帶階層字樣」的判斷

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
    "typo":        ("matched",   "對到（學名疑似錯字）",         "partner"),
    "higher":      ("matched",   "僅對到上階（較來源退階）",     "both"),
    "multiple":    ("unmatched", "多個候選無法判斷",             "partner"),
    "nonstandard": ("unmatched", "非正規學名（sp./cf. 等）",     "partner"),
    "noname":      ("unmatched", "無學名（資料缺漏）",           "partner"),
    "placeholder": ("unmatched", "暫定名（GTDB 等資料庫編號）",  "info"),
    "invalid":     ("unmatched", "非學名（代號、unknown 等）",   "partner"),
    "vetoed":      ("unmatched", "疑應可對到（比對保守否決）",   "review"),
    "rank":        ("unmatched", "階層（rank）與學名不符",       "partner"),
    "conflict":    ("unmatched", "上階層與 TaiCOL 不符",         "both"),
    "none":        ("unmatched", "TaiCOL 尚未收錄",              "ours"),
}

# 給夥伴 CSV 的補充說明（PNG 標籤空間有限，詳細說明放這裡）
CATEGORY_NOTE = {
    "multiple":    "此學名對應到多個分類群（如同名異物），無法判斷是哪一個。"
                   "建議在學名加上命名者（如 Polygonum chinense L.），"
                   "或增加上階層資訊（如界、綱、目、科）以利比對。",
    "nonstandard": "學名含 sp.、cf. 等開放命名標記，且未找到可對應的名稱；"
                   "請確認標記前的屬名／科名是否正確，若已鑑定到種，請更新學名。",
    "rank":        "TaiCOL 有收錄此學名，但階層與來源的 sourceTaxonRank 不一致"
                   "（如三名法卻填「種」、雙名法卻填「科」），請確認階層欄位。",
    "conflict":    "TaiCOL 有同名學名，但上階層（界／綱／目／科）與來源不符。"
                   "請確認上階層是否正確；若確認無誤，可能是 TaiCOL 尚未收錄的同名異物。",
    "placeholder": "此為 BOLD BIN 或 GTDB 等資料庫編號，非正式學名，無法對應 TaiCOL；"
                   "若已鑑定，請提供學名。",
    "invalid":     "學名欄位為代號、數字，或 unknown／未發現等字樣，並非學名。"
                   "請確認是否為欄位錯位；若已鑑定，請填入學名。",
}

# 已對到、但 rank 與學名形式不符（種／種下填錯）：只列在給夥伴的 CSV 提醒，不是分類、不影響比對率
RANK_FORM_LABEL = "階層（rank）與學名不符（已依學名對到）"
RANK_FORM_NOTE = ("學名為種下（三名法或含 var.／subsp. 等）卻填「種」，或為雙名法卻填種下階層，"
                  "與 sourceTaxonRank 不一致。此筆已依學名本身的形式對到，請確認並修正階層欄位。")

# 夥伴可修/需知的未對到類別（none→TaiCOL、vetoed→內部檢討、noname 僅計數不列名、另產逐筆 zip）
PARTNER_CATS = {"typo", "multiple", "nonstandard", "rank", "conflict", "placeholder", "invalid"}
# 算進「對到來源階層」比例的分類（疑似錯字也是對到正確物種，只是拼寫不同）
ATRANK_KEYS = {"atrank", "typo"}

_STAGES = ",".join(f"stage_{i}" for i in range(1, 9))
# 開放命名標記：句點可有可無，但須獨立成詞（避免誤判 sp019039055、species 等）
_NONSTD_RE = r'(^|[[:space:]])(sp|spp|cf|aff|indet)\.?([[:space:]]|$)'
# 開放命名中「只鑑定到前一個名稱」的標記（cf./aff. 為種階層的不確定，不包含在內）
_OPEN_RANK_RE = r'(^|[[:space:]])(sp|spp|indet)\.?([[:space:]]|$)'
# 暫定名：GTDB 等 sp 後接 6 位以上數字，或 BOLD BIN（BOLD:XXX0000）
_PLACEHOLDER_RE = r'[[:space:]]sp[0-9]{6,}|^BOLD:'
# 非學名：純數字／符號（6、18）、單一字母（X）、unknown／unidentified、中文的未發現等字樣。
# 多為欄位錯位或代號，請原單位修正，不歸給 TaiCOL。（以 ~* 比對，不分大小寫）
_INVALID_RE = (r'^[0-9[:space:][:punct:]]+$'
               r'|^[A-Za-z]$'          # 只限拉丁字母；單字中文名（如「雞」）由 INVALID_NAMES 個別指定
               r'|(^|[^[:alpha:]])(unknown|unidentified)'
               r'|未發現|無發現|未調查|無法辨認|鑑定中|未鑑定|未知種'
               r'|^no [a-z]+$')        # 「no bats」這類「沒有發現 X」的紀錄
# 人工維護：regex 不好涵蓋的非學名（俗名、類群名等），整串完全相同才算。
# 發現新的樣態時直接加在這裡；會列入給夥伴的清單，不回報 TaiCOL
INVALID_NAMES = [
    '雞', '羊', '牛',                  # 家畜俗名，請來源提供學名
    '家蝠類群', 'Moss taxa',            # 類群名
]
_INVALID_NAMES_SQL = ", ".join("'" + n.replace("'", "''") + "'" for n in INVALID_NAMES) or "NULL"

# 每筆紀錄的「學名」：依比對流程順序 學名(stage 1) → 中文名(stage 3) → 原始名稱(stage 5)，
# 取第一個非空值（皆空為 NULL）
# originalVernacularName / sourceVernacularName 只在 records，且只有學名空白時才需要，
# 故用相關子查詢以 tbiaID（唯一索引）查找；COALESCE 會短路，學名有值時不會查 records。
NAME_EXPR = """
COALESCE(
  NULLIF(btrim(m."sourceScientificName"), ''),
  (SELECT COALESCE(NULLIF(btrim(r."sourceVernacularName"), ''),
                   NULLIF(btrim(r."originalVernacularName"), ''))
   FROM records r WHERE r."tbiaID" = m."tbiaID")
)
"""
# records 端的同一定義（取來源欄位時使用）
NAME_EXPR_REC = """
COALESCE(NULLIF(btrim(r."sourceScientificName"), ''),
         NULLIF(btrim(r."sourceVernacularName"), ''),
         NULLIF(btrim(r."originalVernacularName"), ''))
"""

# 品質以「來源自己的階層」為基準：match_higher_taxon=True 才算較來源退階；
# 但 sp./spp./indet. 以屬名（stage 4）對到時，對到的即為來源鑑定的階層，算 atrank。
#   （2026-10 起比對端開放命名單名已不走 stage 4；此條保留給尚未重新比對的舊 match_log）
# 未對到時，先看名稱本身：無學名 > 非學名（代號、unknown）> 暫定名；
# 再看 stage_1~8 的 issue code，多個並存的優先序：
#   vetoed > rank > conflict > multiple > nonstandard > none
#   比對端已給出具體原因者優先；rank / conflict 是來源自己的名稱有候選但被淘汰，
#   比後段（如屬名 stage 4）的 multiple 更接近根本原因。沒有其他原因時才歸為非正規學名
# （作用在已算好 name 欄的子查詢上）
CATEGORY_CASE = f"""
CASE
  WHEN "is_matched" THEN
    CASE
      WHEN "match_higher_taxon" AND "match_stage" = 4
           AND name ~* '{_OPEN_RANK_RE}' THEN 'atrank'
      WHEN "match_higher_taxon" THEN 'higher'
      ELSE 'atrank'
    END
  ELSE
    CASE
      WHEN name IS NULL                    THEN 'noname'
      WHEN name ~* '{_INVALID_RE}'
        OR name IN ({_INVALID_NAMES_SQL})  THEN 'invalid'
      WHEN name ~  '{_PLACEHOLDER_RE}'     THEN 'placeholder'
      WHEN 'vetoed'   IN ({_STAGES}) THEN 'vetoed'
      WHEN 'rank'     IN ({_STAGES}) THEN 'rank'
      WHEN 'conflict' IN ({_STAGES}) THEN 'conflict'
      WHEN 'multiple' IN ({_STAGES}) THEN 'multiple'
      WHEN name ~* '{_NONSTD_RE}'          THEN 'nonstandard'
      ELSE 'none'
    END
END
"""


# ── 查詢 ────────────────────────────────────────────────────
# ── 資料庫（單位）對照：rights_holder → (group, info_id) ─────────────
# 同 rematch_all 的 HOLDERS；info_id 對應 portal Partner.info 的 id，輸出目錄為 {group}_{info_id}
# 新增資料庫時要一併補上，否則該資料庫不會產出報告（執行時會提示）
UNITS = [
    # (rights_holder, group, info_id)
    ('集水區友善環境生態資料庫',                'ardswc',      0),
    ('臺灣魚類資料庫',                          'ascdc',       0),
    ('中央研究院生物多樣性中心動物標本館',        'asiz',        0),
    ('中油生態地圖',                            'cpc',         0),
    ('林業試驗所昆蟲標本館',                     'fact',        0),
    ('生態調查資料庫系統',                       'forest',      0),
    ('GBIF',                                   'gbif',        0),
    ('中央研究院生物多樣性中心植物標本資料庫',     'hast',        0),
    ('愛自然-臺灣(iNaturalist Taiwan)',         'ntuforestry', 0),
    ('國立海洋生物博物館生物典藏管理系統',        'nmmba',       0),
    ('科博典藏 (NMNS Collection)',              'nmns',        0),
    ('國家海洋資料庫及共享平台',                  'namr',        0),
    ('作物種原資訊系統',                         'npgrc',       0),
    ('臺灣國家公園生物多樣性資料庫',              'nps',         0),
    ('國立臺灣博物館典藏',                       'ntm',         0),
    ('海洋保育資料倉儲系統',                     'oca',         0),
    ('臺灣生物多樣性資訊機構 TaiBIF',             'brcas',       0),
    ('林業試驗所植物標本資料庫',                  'taif',        0),
    ('台灣生物多樣性網絡 TBN',                   'tbri',        0),
    ('濕地環境資料庫',                           'nps',         1),
    ('河川環境資料庫',                           'wra',         0),
]


def unit_key(group, info_id):
    return f"{group}_{info_id}"


def warn_unknown_holders(conn):
    """match_log 中有、但 UNITS 沒列到的 rights_holder → 提示補上。"""
    rows = conn.execute(text('SELECT DISTINCT "rights_holder" FROM match_log'))
    known = {u[0] for u in UNITS}
    for (rh,) in rows:
        if rh and rh not in known:
            print(f"⚠️ UNITS 未包含 rights_holder：{rh}（不會產出報告）", file=sys.stderr)


def prepare(conn, rhs):
    """一次掃描 match_log、records，建立暫存表供各資料庫使用（同一連線內有效）。

    _mr_agg：依 (資料庫, 學名, 分類, taxonID[僅 higher 與模糊對到], 逐筆欄位[僅 noname], 是否模糊對到) 彙總的筆數
    _mr_src：未對到紀錄的來源欄位，每個 (資料庫, 學名) 取一筆代表
    """
    conn.execute(text("SET work_mem = '256MB'"))
    conn.execute(text("DROP TABLE IF EXISTS _mr_agg, _mr_src"))

    print("彙整 match_log（掃描一次）...", file=sys.stderr)
    conn.execute(text(f"""
        CREATE TEMP TABLE _mr_agg AS
        SELECT rh, name, cat,
               CASE WHEN cat = 'higher' OR fz THEN "taxonID" END  AS taxon_id,
               CASE WHEN cat = 'noname' THEN "tbiaID" END         AS tbia_id,
               CASE WHEN cat = 'noname' THEN "occurrenceID" END   AS occurrence_id,
               CASE WHEN cat = 'noname' THEN "catalogNumber" END  AS catalog_number,
               fz, rk,
               COUNT(*) AS records
        FROM (
            SELECT rh, name, "taxonID", "tbiaID", "occurrenceID", "catalogNumber",
                   {CATEGORY_CASE} AS cat,
                   -- 模糊對到（分數 < 1）；之後在 Python 依字串差異分成疑似錯字、字尾變化等
                   COALESCE("is_matched" AND 'fuzzy' IN ({_STAGES}), false) AS fz,
                   -- 已對到但 stage 有 rank:種／種下的 rank 與學名形式不符,比對端依學名形式對到
                   COALESCE("is_matched" AND 'rank' IN ({_STAGES}), false) AS rk
            FROM (
                SELECT m."rights_holder" AS rh, {NAME_EXPR} AS name,
                       m."is_matched", m."match_higher_taxon", m."match_stage",
                       {", ".join(f'm."stage_{i}"' for i in range(1, 9))},
                       m."taxonID", m."tbiaID", m."occurrenceID", m."catalogNumber"
                FROM match_log m
                WHERE m."rights_holder" = ANY(:rhs)
            ) a
        ) b
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    """), {"rhs": list(rhs)})
    conn.execute(text("CREATE INDEX ON _mr_agg (rh, cat)"))

    print("彙整 records 來源欄位（掃描一次）...", file=sys.stderr)
    conn.execute(text(f"""
        CREATE TEMP TABLE _mr_src AS
        SELECT DISTINCT ON (rh, name) rh, name, vern, rank, fam, ord, cls, kin
        FROM (
            SELECT r."rightsHolder" AS rh, {NAME_EXPR_REC} AS name,
                   r."sourceVernacularName" AS vern, r."sourceTaxonRank" AS rank,
                   r."sourceFamily" AS fam, r."sourceOrder" AS ord,
                   r."sourceClass" AS cls, r."sourceKingdom" AS kin
            FROM records r
            WHERE r."taxonID" IS NULL AND r."rightsHolder" = ANY(:rhs)
        ) s
        WHERE name IS NOT NULL
        ORDER BY rh, name
    """), {"rhs": list(rhs)})
    conn.execute(text("CREATE INDEX ON _mr_src (rh)"))
    conn.execute(text("ANALYZE _mr_agg; ANALYZE _mr_src"))
    conn.commit()


def aggregate(conn, rh):
    """回傳 [(category_key, records, unique_names), ...]。"""
    sql = text("""
        SELECT cat, SUM(records) AS records, COUNT(DISTINCT name) AS unique_names
        FROM _mr_agg WHERE rh = :g
        GROUP BY cat
    """)
    return [(r[0], int(r[1]), int(r[2])) for r in conn.execute(sql, {"g": rh})]


def unmatched_names(conn, rh):
    """未對到的唯一學名清單（給 CSV），依影響筆數排序。"""
    sql = text("""
        SELECT name, cat, SUM(records) AS records
        FROM _mr_agg
        WHERE rh = :g AND cat NOT IN ('atrank', 'higher')
        GROUP BY name, cat
        ORDER BY records DESC
    """)
    return [(r[0], r[1], int(r[2])) for r in conn.execute(sql, {"g": rh})]


def higher_names(conn, rh):
    """有對到但只退到上階的唯一學名，帶對到的 taxonID。
    （sp./spp./indet. 以屬名對到者在 CATEGORY_CASE 已算 atrank，不會出現在這裡）"""
    sql = text("""
        SELECT name, taxon_id, SUM(records) AS records
        FROM _mr_agg
        WHERE rh = :g AND cat = 'higher'
        GROUP BY name, taxon_id
        ORDER BY records DESC
    """)
    return [(r[0], r[1], int(r[2])) for r in conn.execute(sql, {"g": rh})]


_SRC_EMPTY = {"vern": "", "rank": "", "fam": "", "ord": "", "cls": "", "kin": ""}


def rank_form_names(conn, rh):
    """已對到、但來源 rank 與學名形式不符(種／種下填錯)的學名:[(name, records)]。"""
    sql = text("""
        SELECT name, SUM(records) FROM _mr_agg
        WHERE rh = :g AND rk AND name IS NOT NULL
        GROUP BY name
    """)
    return [(r[0], int(r[1])) for r in conn.execute(sql, {"g": rh})]


def rank_form_sources(conn, rights_holder, names):
    """rank_form_names 的來源欄位(_mr_src 只有未對到的紀錄,這些另外查)。"""
    if not names:
        return {}
    sql = text(f"""
        SELECT DISTINCT ON (name) name, vern, rank, fam, ord, cls, kin FROM (
            SELECT {NAME_EXPR_REC} AS name,
                   r."sourceVernacularName" AS vern, r."sourceTaxonRank" AS rank,
                   r."sourceFamily" AS fam, r."sourceOrder" AS ord,
                   r."sourceClass" AS cls, r."sourceKingdom" AS kin
            FROM records r WHERE r."rightsHolder" = :rh
        ) s WHERE name = ANY(:names)
        ORDER BY name
    """)
    return {r.name: {k: (getattr(r, k) or "") for k in _SRC_EMPTY}
            for r in conn.execute(sql, {"rh": rights_holder, "names": list(names)})}


def source_fields_of(conn, rh):
    """該資料庫每個未對到學名的代表來源欄位（中文名、階層、各上階）。"""
    sql = text("SELECT name, vern, rank, fam, ord, cls, kin FROM _mr_src WHERE rh = :g")
    return {r.name: {k: (getattr(r, k) or "") for k in _SRC_EMPTY}
            for r in conn.execute(sql, {"g": rh})}


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
_PLACEHOLDER_PY = re.compile(r'\ssp\d{6,}|^BOLD:')
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
    if _CJK.search(name):
        # 沒有學名、以中文名比對：學名格式相關的檢查不適用
        hints.append("無學名（以中文名比對）")
        if not any(s.get(k) for k in ("fam", "ord", "cls", "kin")):
            hints.append("無上階層資訊")
        return "；".join(hints)
    if RANK_PREFIX_RE.match(name):
        hints.append("學名含階層字樣（如 Family）")
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


# ── 模糊對到的分類 ──────────────────────────────────────────
# 比對端分數 < 1 但有對到的學名，依「來源學名」與「TaiCOL 學名」的字串差異分成：
#   typo    疑似錯字：去掉作者與階層標記後只差 1～2 個字元（且 ≤ 15%）→ 給夥伴
#   ending  種小名字尾性別變化（formosana↔formosanus）→ 給 TaiCOL（可收錄為異名）
#   format  只差作者名、階層標記、自名亞種寫法、連字號 → 不列
#   other   同物異名、改屬等 → 不列
# 都算「對到」；typo 從 atrank 拆成獨立分類，ending／format／other 仍為 atrank。
_RANK_MARK = {'var', 'subsp', 'ssp', 'f', 'forma', 'fo', 'cv', 'x', '\u00d7', 'subvar'}
_ENDINGS = {'us', 'a', 'um', 'is', 'e', 'ii', 'i', 'ae', 'ensis', 'ense', 'es', 'ior', 'ius',
            'os', 'on'}                                   # os/on：希臘字尾（heterocarpos、adenocaulon）
# 單名的階層字尾（亞科 -inae、科 -idae 等）；只差這個不是錯字，是階層不同
_RANK_SUFFIX = ('oideae', 'aceae', 'oidea', 'idae', 'inae', 'ales', 'ini', 'ina')


def _sci_tokens(name):
    """只留學名本體：屬名＋小寫的種小名／種下名；去掉作者、年份、階層標記。"""
    toks = re.sub(r'[()\[\],]', ' ', name or '').split()
    out = []
    for i, t in enumerate(toks):
        t2 = t.rstrip('.').lower()
        if i == 0:
            out.append(t2)
            continue
        if t2 in _RANK_MARK:
            continue
        if not re.fullmatch(r'[a-z\-]+', t.rstrip('.')):   # 遇到作者（大寫、縮寫、年份）就停
            break
        out.append(t2)
    return out


def _lev(a, b):
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _ending_only(x, y):
    """兩個種小名是否只差常見字尾：x = 字幹 + 字尾1、y = 字幹 + 字尾2，字幹至少 4 字元。
    （逐一試字尾，避免共同字首吃掉字尾的一部分，如 immaculatum／immaculatus）"""
    for e1 in _ENDINGS | {''}:
        if not x.endswith(e1):
            continue
        stem = x[:len(x) - len(e1)]
        if len(stem) >= 4 and y.startswith(stem) and (y[len(stem):] in _ENDINGS or y == stem):
            return True
    return False


def _rank_suffix(w):
    return next((suf for suf in _RANK_SUFFIX if w.endswith(suf)), None)


def classify_fuzzy(src, tgt):
    a, b = _sci_tokens(src), _sci_tokens(tgt)
    if not a or not b:
        return 'other'
    if a == b:
        return 'format'
    if [t.replace('-', '') for t in a] == [t.replace('-', '') for t in b]:
        return 'format'                                   # 只差連字號（rubro-pilosum→rubropilosum）：寫法標準化，不是錯字
    if len(a) == len(b) + 1 and len(a) >= 3 and a[-1] == a[-2] and a[:-1] == b:
        return 'format'                                   # 自名亞種 X y y → X y
    if len(a) == len(b) and a[0] == b[0] and all(p == q or _ending_only(p, q) for p, q in zip(a[1:], b[1:])):
        return 'ending'                                   # 屬名相同、種小名以下只差字尾
    if len(a) == len(b) == 1 and _rank_suffix(a[0]) and _rank_suffix(b[0]) \
            and _rank_suffix(a[0]) != _rank_suffix(b[0]):
        return 'other'                                    # 單名階層字尾不同（Ardeinae→Ardeidae）：階層不同，不是錯字
    sa, sb = ' '.join(a), ' '.join(b)
    d = _lev(sa, sb)
    if d <= 2 and d <= 0.15 * max(len(sa), len(sb)):
        return 'typo'
    return 'other'


def split_fuzzy(conn, rh):
    """把該資料庫模糊對到（且對到來源階層）的學名分類；typo 直接在 _mr_agg 改成 cat='typo'。
    回傳 (typo_map {學名: TaiCOL 學名},
          fuzzy_rows {'typo': [...], 'ending': [...]}，每筆為 (學名, taxonID, 筆數, TaiCOL taxa doc))。"""
    rows = conn.execute(text("""
        SELECT name, taxon_id, SUM(records) FROM _mr_agg
        WHERE rh = :g AND cat = 'atrank' AND fz AND name IS NOT NULL AND taxon_id IS NOT NULL
        GROUP BY 1, 2
    """), {"g": rh}).fetchall()
    if not rows:
        return {}, {'typo': [], 'ending': []}
    docs = _solr_lookup("id", [r[1] for r in rows],
                        ["scientificName", "common_name_c", "family", "family_c"])
    typo_pairs, typo_map, fuzzy_rows = [], {}, {'typo': [], 'ending': []}
    for name, tid, records in rows:
        d = docs.get(tid)
        tname = _first(d.get("scientificName")) if d else ""
        if not tname:
            continue                                      # 查不到 TaiCOL 學名就不分類，維持 atrank
        kind = classify_fuzzy(name, tname)
        if kind == 'typo':
            typo_pairs.append({"n": name, "t": tid})
            typo_map[name] = tname
        if kind in fuzzy_rows:
            fuzzy_rows[kind].append((name, tid, int(records), d))
    if typo_pairs:
        # 用陣列一次更新（不用 executemany：連線可能被設成 stream_results，伺服器端游標不支援）
        conn.execute(text("""
            UPDATE _mr_agg a SET cat = 'typo'
            FROM (SELECT unnest(CAST(:names AS text[])) AS n, unnest(CAST(:tids AS text[])) AS t) v
            WHERE a.rh = :g AND a.cat = 'atrank' AND a.fz AND a.name = v.n AND a.taxon_id = v.t
        """), {"g": rh, "names": [p["n"] for p in typo_pairs], "tids": [p["t"] for p in typo_pairs]})
        conn.commit()
    return typo_map, fuzzy_rows


# ── 產出 ────────────────────────────────────────────────────
def write_snapshot(agg, group, info_id, rights_holder, year_month, out_dir):
    rows = []
    for key, records, uniq in agg:
        axis, label, resp = CATEGORY_META.get(key, ("unmatched", key, None))
        rows.append({
            "group": group, "info_id": info_id, "rights_holder": rights_holder,
            "year_month": year_month, "axis": axis, "category_key": key, "category": label, "responsibility": resp,
            "records": int(records), "unique_names": int(uniq),
        })
    (out_dir / "snapshot.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return rows


def write_noname_records(conn, rh, out_dir):
    """無學名（學名、中文名、原始名稱皆空白）且未對到的逐筆清單，供夥伴補學名。
    以 tbiaID 回查 records 補欄位；串流寫進 zip，不一次載入記憶體。沒有資料時不產出檔案。"""
    path = out_dir / "noname_records.zip"      # 內含 noname_records.csv
    if path.exists():
        path.unlink()
    sql = text("""
        SELECT a.tbia_id, a.occurrence_id, a.catalog_number,
               r."sourceTaxonRank", r."sourceFamily", r."sourceOrder", r."sourceClass",
               r."sourceKingdom", r."datasetName"
        FROM _mr_agg a
        LEFT JOIN records r ON r."tbiaID" = a.tbia_id
        WHERE a.rh = :g AND a.cat = 'noname'
        ORDER BY a.tbia_id
    """)
    head = ["tbiaID", "occurrenceID", "catalogNumber", "階層(來源)",
            "科(來源)", "目(來源)", "綱(來源)", "界(來源)", "資料集名稱"]
    n, zf, f, w = 0, None, None, None
    result = conn.execution_options(stream_results=True).execute(sql, {"g": rh})
    try:
        for rows in iter(lambda: result.fetchmany(10000), []):
            if zf is None:
                zf = zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED)
                f = io.TextIOWrapper(zf.open("noname_records.csv", "w"),
                                     encoding="utf-8-sig", newline="")
                w = csv.writer(f)
                w.writerow(head)
            w.writerows([[v if v is not None else "" for v in r] for r in rows])
            n += len(rows)
    finally:
        result.close()
        conn.execution_options(stream_results=False)   # 還原：execution_options 會改到整條連線
        if f:
            f.close()
        if zf:
            zf.close()
    return n


def name_sort_key(name):
    """清單依學名字母排序（不分大小寫）；中文名等非拉丁字排在拉丁學名之後。"""
    name = name or ""
    return (name.casefold(), name)


def write_csvs(conn, rh, rights_holder, out_dir, typo_map=None, fuzzy_rows=None):
    """拆兩份：unmatched_partner.csv（夥伴可修）＋ for_taicol.csv（回報 TaiCOL）。
    兩份都附來源階層與格式提醒，方便判斷是否為格式問題；都依學名字母排序，方便閱讀。"""
    un = unmatched_names(conn, rh)      # (name, catkey, records)
    hi = higher_names(conn, rh)         # (name, taxon_id, records)
    label = {k: v[1] for k, v in CATEGORY_META.items()}

    src = source_fields_of(conn, rh)
    by_tid = _solr_lookup("id", [r[1] for r in hi],  # taxa core 主鍵為 id，值即 taxonID
                          ["common_name_c", "family", "family_c"])

    src_head = ["中文名(來源)", "階層(來源)", "科(來源)", "目(來源)", "綱(來源)", "界(來源)"]
    src_row = lambda s: [s["vern"], s["rank"], s["fam"], s["ord"], s["cls"], s["kin"]]

    # 給夥伴：PARTNER_CATS 各類別 ＋ 已對到但 rank 與學名形式不符（提醒，不影響比對率），依學名字母排序
    out_rows = []
    for name, cat, records in un:
        if cat in PARTNER_CATS:
            s = src.get(name, _SRC_EMPTY)
            note = CATEGORY_NOTE.get(cat, "")
            if cat == "typo":
                note = (f"與 TaiCOL 收錄的學名「{(typo_map or {}).get(name, '')}」拼寫只差 1～2 個字元，"
                        "請確認是否為錯字；若貴單位的拼寫才正確，請告知，我們會回報 TaiCOL。"
                        "此筆已依 TaiCOL 學名對到。")
            out_rows.append([name, label.get(cat, cat), note, records]
                            + src_row(s) + [_format_hints(name, s)])
    rf = rank_form_names(conn, rh)
    rf_src = rank_form_sources(conn, rights_holder, [n for n, _ in rf])
    for name, records in rf:
        s = rf_src.get(name, _SRC_EMPTY)
        out_rows.append([name, RANK_FORM_LABEL, RANK_FORM_NOTE, records]
                        + src_row(s) + [_format_hints(name, s)])
    out_rows.sort(key=lambda r: (name_sort_key(r[0]), r[1]))
    with open(out_dir / "unmatched_partner.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["學名", "原因分類", "說明", "影響筆數"] + src_head + ["格式提醒"])
        w.writerows(out_rows)

    # 給 TaiCOL：未收錄（none）＋ 僅對到上階（higher）＋ 字尾變化 ＋ 拼寫不同；排除非正規學名與暫定名；依學名字母排序
    rows = []
    for name, cat, records in un:
        if cat == "none" and not _is_nonstandard(name):
            s = src.get(name, _SRC_EMPTY)
            rows.append([name, "TaiCOL 尚未收錄", records, "", s["vern"], s["fam"],
                         s["rank"], s["kin"], _format_hints(name, s), rights_holder])
    for name, tid, records in hi:
        if _is_nonstandard(name):
            continue
        d = by_tid.get(tid, {})
        cn = _first(d.get("common_name_c"))
        fam = _first(d.get("family_c")) or _first(d.get("family"))
        rows.append([name, "僅對到上階", records, tid, cn, fam,
                     "", "", "", rights_holder])
    # 已對到、但拼寫與 TaiCOL 不同者，TaiCOL 也看一份：
    #   字尾變化（formosana↔formosanus）：可評估收錄為異名
    #   拼寫不同（疑似錯字，夥伴也會收到）：也可能是 TaiCOL 端拼錯，請 TaiCOL 一併確認
    for kind, label_ in (("ending", "字尾變化"), ("typo", "拼寫不同")):
        for name, tid, records, d in (fuzzy_rows or {}).get(kind, []):
            rows.append([name, label_, records, tid, _first(d.get("common_name_c")),
                         _first(d.get("family_c")) or _first(d.get("family")),
                         "", "", f"TaiCOL 學名：{_first(d.get('scientificName'))}", rights_holder])
    rows.sort(key=lambda r: (name_sort_key(r[0]), r[1]))
    with open(out_dir / "for_taicol.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["學名", "類別", "影響筆數", "對到的taxonID", "中文名", "科",
                    "階層(來源)", "界(來源)", "格式提醒", "rights_holder"])
        w.writerows(rows)


# ── 前後比較 ────────────────────────────────────────────────
def prev_dir_of(unit, year_month):
    """找本次之前、最近一次有該資料庫快照的月份；回 (year_month, 目錄) 或 (None, None)。
    unit 為 {group}_{info_id}；舊格式（以 group 為目錄）的報告不列入比較。"""
    if not OUT_BASE.exists():
        return None, None
    cands = sorted(p.name for p in OUT_BASE.iterdir()
                   if re.fullmatch(r"\d{4}-\d{2}", p.name) and p.name < year_month
                   and (p / unit / "snapshot.json").exists())
    return (cands[-1], OUT_BASE / cands[-1] / unit) if cands else (None, None)


def name_states(conn, rh):
    """每個學名的主要分類（筆數最多者）與總筆數：{name: (cat, records)}。"""
    sql = text("""
        SELECT name, cat, SUM(records) AS records
        FROM _mr_agg WHERE rh = :g
        GROUP BY name, cat
    """)
    acc = {}   # name -> (主要分類, 主要分類筆數, 總筆數)
    for name, cat, records in conn.execute(sql, {"g": rh}):
        name, records = name or "", int(records)
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
    # 疑似錯字（typo）也是對到來源階層，比較時視同 atrank，避免 atrank→typo 被當成「變差」
    same = lambda c: "atrank" if c in ATRANK_KEYS else c
    rows = []
    for name, (cat, rec) in states.items():
        old = prev_states.get(name)
        if old is None:
            if same(cat) != "atrank":                # 新出現且未達來源階層才列
                rows.append((name, "新增學名", "", cat, 0, rec))
            continue
        ocat, orec = old
        if same(ocat) == same(cat):
            continue
        if same(cat) == "atrank":
            kind = "新對到來源階層"
        elif same(ocat) == "atrank":
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


def write_meta(out_dir, group, info_id, rights_holder, year_month, prev_ym,
               logic_changed, compare_summary):
    """本次產出資訊，供 stat_match 匯入 MatchReport。"""
    meta = {
        "group": group, "info_id": info_id, "rights_holder": rights_holder,
        "year_month": year_month,
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
    ("對到（學名疑似錯字）", "對到但拼寫不同", "#5b9a86"),
    ("僅對到上階（較來源退階）", "較來源退階", "#a7c4b7"),
    ("__unmatched__", "未對到", "#c2cbc4"),
]


def _is_atrank(r):
    return r.get("category_key") in ATRANK_KEYS or r.get("category") == "對到（來源階層）"


def _atrank_rate(rows):
    total = sum(r["records"] for r in rows) or 1
    at = sum(r["records"] for r in rows if _is_atrank(r))
    return round(at / total * 100, 1)


def _pct_text(n, total):
    """有資料但不到 0.1% 顯示「<0.1%」，未滿 100% 但四捨五入成 100 顯示「>99.9%」（同網頁）。"""
    if not total:
        return "0%"
    p = n / total * 100
    if n > 0 and p < 0.1:
        return "<0.1%"
    if n < total and p >= 99.95:
        return ">99.9%"
    return f"{p:.1f}%"


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
    atrank_n = sum(r["records"] for r in snapshot_rows if _is_atrank(r))   # 含疑似錯字
    ax.text(5, 74, _pct_text(atrank_n, total), fontproperties=BOLD, fontsize=38, color=INK)
    ax.text(5, 67.5, "對到來源提供的階層", fontproperties=SANS, fontsize=9.5, color=MUTED)
    ax.text(42, 77, f"整體對到 {_pct_text(matched, total)}", fontproperties=SANS, fontsize=9, color=MUTED)
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
        ax.add_patch(plt.Rectangle((lx, 41), 2, 2, color=color))
        t = f"{short} {_pct_text(val, total)}"
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
        if higher:
            ax.text(5, 27, f"本次資料全數對到 TaiCOL，其中 {_pct_text(higher, total)} 較來源退階（僅對到上階）。",
                    fontproperties=SANS, fontsize=9, color=INK)
            ax.text(5, 21, "多因 TaiCOL 尚未收錄該物種、或來源缺上階資訊；名單詳見附件。",
                    fontproperties=SANS, fontsize=9, color=MUTED)
        else:
            ax.text(5, 24, "本次資料全數對到來源提供的階層，品質良好。",
                    fontproperties=SANS, fontsize=9.5, color=INK)
    fig.savefig(out_dir / "email.png", facecolor=PAPER)
    plt.close(fig)


# ── 主流程 ──────────────────────────────────────────────────
def process_unit(conn, rh, group, info_id, year_month, show_delta=True):
    unit = unit_key(group, info_id)
    out_dir = OUT_BASE / year_month / unit
    out_dir.mkdir(parents=True, exist_ok=True)

    typo_map, fuzzy_rows = split_fuzzy(conn, rh)      # 先把疑似錯字從 atrank 拆出來
    agg = aggregate(conn, rh)
    if not agg:
        print(f"[{unit}] {rh}｜match_log 無資料，略過", file=sys.stderr)
        return
    snapshot = write_snapshot(agg, group, info_id, rh, year_month, out_dir)
    write_csvs(conn, rh, rh, out_dir, typo_map, fuzzy_rows)
    write_noname_records(conn, rh, out_dir)

    states = name_states(conn, rh)
    write_name_state(states, out_dir)

    prev_ym, prev_dir = prev_dir_of(unit, year_month)
    prev_rate, compare_summary = None, None
    if prev_dir:
        compare_summary = write_compare(snapshot, states, prev_ym, prev_dir, out_dir)
        if show_delta:
            prev_rate = _atrank_rate(json.loads(
                (prev_dir / "snapshot.json").read_text(encoding="utf-8")))
    write_meta(out_dir, group, info_id, rh, year_month, prev_ym, not show_delta, compare_summary)
    render_png(snapshot, rh, year_month, prev_rate, prev_ym, out_dir)

    total = sum(r["records"] for r in snapshot)
    matched = sum(r["records"] for r in snapshot if r["axis"] == "matched")
    rate = round(matched / total * 100, 1) if total else 0.0
    print(f"[{unit}] {rh}｜比對率 {rate}%｜{total} 筆｜疑似錯字 {len(typo_map)} 個、字尾變化 {len(fuzzy_rows['ending'])} 個學名"
          f"｜前次 {prev_ym or '無'} → {out_dir}",
          file=sys.stderr)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year-month", default=f"{datetime.date.today():%Y-%m}",
                   help="YYYY-MM，預設本月")
    p.add_argument("--group", nargs="+",
                   help="只處理這些 group 的資料庫，可傳多個（如 --group nps namr）")
    p.add_argument("--unit", nargs="+",
                   help="只處理這些資料庫，格式 {group}_{info_id}，可傳多個（如 --unit nps_1 namr_0）")
    p.add_argument("--no-delta", action="store_true",
                   help="比對邏輯變更後的首次產出：PNG 不顯示差值，meta.json 標記 logic_changed，"
                        "網頁同樣隱藏差值；比較檔仍會產出")
    args = p.parse_args()

    units = [u for u in UNITS
             if (not args.group or u[1] in args.group)
             and (not args.unit or unit_key(u[1], u[2]) in args.unit)]
    if not units:
        sys.exit(f"找不到符合的資料庫：group={args.group} unit={args.unit}")

    with engine.connect() as conn:
        if not args.group and not args.unit:
            warn_unknown_holders(conn)
        prepare(conn, [u[0] for u in units])
        for rh, group, info_id in units:
            process_unit(conn, rh, group, info_id, args.year_month, show_delta=not args.no_delta)
    print(f"完成 {len(units)} 個資料庫", file=sys.stderr)


if __name__ == "__main__":
    main()