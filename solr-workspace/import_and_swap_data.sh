#!/bin/bash
# 藍綠 SWAP 匯入：CSV 一律灌入 BUILD_CORE，確認無誤後與 LIVE_CORE 原子對調。
# 取代 import_and_delete_data.sh（就地覆蓋 + 依 modified 刪除 stale）。
#
# 用法（建議兩段式，只灌一次）：
#   ACTION=import DRY_RUN=false ./import_and_swap_data.sh   # 清空 build core 後灌入，不切換
#   ACTION=swap   DRY_RUN=false ./import_and_swap_data.sh   # 檢查通過後 SWAP
#   ACTION=all    DRY_RUN=false ./import_and_swap_data.sh   # 灌入 + 切換一次做完
# DRY_RUN=true（預設）只顯示將要做的事與目前筆數，不清空、不匯入、不切換。
#
# 回滾：curl "${SOLR_HOST}/solr/admin/cores?action=SWAP&core=tbia_records&other=tbia_records_build"
#       （下一輪 import 前都可以；import 開頭會清空 build core）
set -eo pipefail

# ===== 設定 (可透過環境變數覆寫) =====
CSV_DIR="${CSV_DIR:-/var/solr/csvs/export}"
CSV_PATTERN="${CSV_PATTERN:-*.csv}"
SOLR_HOST="${SOLR_HOST:-http://solr:8983}"
LIVE_CORE="${LIVE_CORE:-tbia_records}"          # 對外，portal 打這個
BUILD_CORE="${BUILD_CORE:-tbia_records_build}"  # 每輪灌入
MIN_EXPECTED_COUNT="${MIN_EXPECTED_COUNT:-1000000}"
MAX_DROP_PCT="${MAX_DROP_PCT:-10}"   # build 筆數比 live 少超過此百分比就拒絕切換
ALLOW_SCHEMA_DIFF="${ALLOW_SCHEMA_DIFF:-false}"  # v2 切換時設 true（兩邊 schema 本來就不同）
FORCE="${FORCE:-false}"              # 略過切換前檢查（不建議）
MAX_JOBS="${MAX_JOBS:-6}"
DRY_RUN="${DRY_RUN:-true}"
ACTION="${ACTION:-all}"              # all | import | swap
FAIL_LOG="${FAIL_LOG:-/bucket/tbia_import_failures.txt}"
PROGRESS_STEPS="${PROGRESS_STEPS:-50}"

echo "Action: $ACTION | live: $LIVE_CORE | build: $BUILD_CORE | dry run: $DRY_RUN"

case "$ACTION" in
    all|import|swap) ;;
    *) echo "ERROR: ACTION must be all | import | swap (got: $ACTION)"; exit 1;;
esac

if [ "$LIVE_CORE" = "$BUILD_CORE" ]; then
    echo "ERROR: LIVE_CORE and BUILD_CORE are the same ($LIVE_CORE); refusing to continue"
    exit 1
fi

# ===== 共用函式 =====
core_exists() {
    curl -s "${SOLR_HOST}/solr/admin/cores?action=STATUS&core=$1&wt=json" \
        | grep -qE "\"name\"[[:space:]]*:[[:space:]]*\"$1\""
}

core_count() {
    curl -s "${SOLR_HOST}/solr/$1/select?q=*:*&rows=0&wt=json" \
        | grep -oP '"numFound":\s*\K\d+' || true
}

# 欄位名稱清單的雜湊，用來確認兩個 core 的 schema 一致
schema_hash() {
    curl -s "${SOLR_HOST}/solr/$1/schema/fields?wt=json" \
        | grep -oP '"name"\s*:\s*"[^"]+"' | tr -d ' ' | sort | md5sum | cut -d' ' -f1
}

for c in "$LIVE_CORE" "$BUILD_CORE"; do
    if ! core_exists "$c"; then
        echo "ERROR: core not found: $c (see Part A one-time setup)"
        exit 1
    fi
done

# ===== 1. 匯入 build core =====
if [ "$ACTION" = "import" ] || [ "$ACTION" = "all" ]; then
    cd "$CSV_DIR"
    CSV_COUNT=$(find . -maxdepth 1 -name "$CSV_PATTERN" | wc -l)
    echo "Found $CSV_COUNT csv files in $CSV_DIR (pattern: $CSV_PATTERN)"

    if [ "$CSV_COUNT" -eq 0 ]; then
        echo "ERROR: no csv files found in $CSV_DIR"
        exit 1
    fi

    if [ "$DRY_RUN" = "true" ]; then
        echo "[DRY RUN] Would clear $BUILD_CORE (now $(core_count "$BUILD_CORE") records) and import $CSV_COUNT files"
        echo "[DRY RUN] $LIVE_CORE now has $(core_count "$LIVE_CORE") records (untouched)"
        echo "[DRY RUN] Set DRY_RUN=false to actually import"
        if [ "$ACTION" = "import" ]; then exit 0; fi
    else
        # 清空 build core：每輪都是全新索引，不累積 deleted docs
        echo "Clearing $BUILD_CORE..."
        curl -s -f "${SOLR_HOST}/solr/${BUILD_CORE}/update?commit=true" \
            -H "Content-Type: text/xml" \
            --data-binary "<delete><query>*:*</query></delete>" > /dev/null
        echo "$BUILD_CORE after clear: $(core_count "$BUILD_CORE") records"

        echo "Importing into $BUILD_CORE using $MAX_JOBS concurrent jobs..."
        : > "$FAIL_LOG"
        export SOLR_HOST BUILD_CORE FAIL_LOG

        STEP=$(( CSV_COUNT / PROGRESS_STEPS ))
        if [ "$STEP" -lt 1 ]; then STEP=1; fi

        # 失敗的檔案略過並記錄到 $FAIL_LOG，單檔失敗不中止整批（但會阻擋切換）
        find . -maxdepth 1 -name "$CSV_PATTERN" -print0 \
            | xargs -0 -P "$MAX_JOBS" -I {} bash -c '
                f="$1"
                if curl -s -o /dev/null -f -X POST -H "Content-Type: text/csv" \
                     --data-binary @"$f" "${SOLR_HOST}/solr/${BUILD_CORE}/update"; then
                    printf "OK\n"
                else
                    printf "%s\n" "$f" >> "$FAIL_LOG"
                    printf "FAIL\n"
                fi
            ' _ {} \
            | {
                count=0; ok=0; fail=0
                while IFS= read -r line; do
                    count=$((count + 1))
                    case "$line" in
                        OK)   ok=$((ok + 1));;
                        FAIL) fail=$((fail + 1));;
                    esac
                    if [ $((count % STEP)) -eq 0 ] || [ "$count" -eq "$CSV_COUNT" ]; then
                        echo "目前匯入進度: $count / $CSV_COUNT (成功 $ok, 失敗 $fail)"
                    fi
                done
            }

        FAIL_TOTAL=$(wc -l < "$FAIL_LOG" | tr -d ' ')
        if [ "$FAIL_TOTAL" -gt 0 ]; then
            echo "WARNING: $FAIL_TOTAL 個檔案匯入失敗，清單見 $FAIL_LOG（切換會被阻擋）"
        fi

        # ===== 2. Commit（一次落盤並開新 searcher）=====
        echo "Committing $BUILD_CORE..."
        curl -s -f "${SOLR_HOST}/solr/${BUILD_CORE}/update?commit=true&openSearcher=true" > /dev/null

        BUILD_COUNT=$(core_count "$BUILD_CORE")
        echo "After import: $BUILD_CORE has $BUILD_COUNT records ($LIVE_CORE has $(core_count "$LIVE_CORE"))"

        if [ -z "$BUILD_COUNT" ] || [ "$BUILD_COUNT" -lt "$MIN_EXPECTED_COUNT" ]; then
            echo "ERROR: count too low ($BUILD_COUNT < $MIN_EXPECTED_COUNT); not swapping"
            exit 1
        fi
    fi
fi

# ===== 3. 切換前檢查 + SWAP =====
if [ "$ACTION" = "swap" ] || [ "$ACTION" = "all" ]; then
    LIVE_COUNT=$(core_count "$LIVE_CORE")
    BUILD_COUNT=$(core_count "$BUILD_CORE")
    echo "Before swap: $LIVE_CORE=$LIVE_COUNT, $BUILD_CORE=$BUILD_COUNT"

    ERRORS=()
    if [ -z "$BUILD_COUNT" ] || [ "$BUILD_COUNT" -lt "$MIN_EXPECTED_COUNT" ]; then
        ERRORS+=("build count too low ($BUILD_COUNT < $MIN_EXPECTED_COUNT)")
    fi
    if [ -n "$LIVE_COUNT" ] && [ -n "$BUILD_COUNT" ] && [ "$LIVE_COUNT" -gt 0 ]; then
        MIN_ALLOWED=$(( LIVE_COUNT * (100 - MAX_DROP_PCT) / 100 ))
        if [ "$BUILD_COUNT" -lt "$MIN_ALLOWED" ]; then
            ERRORS+=("build count drops more than ${MAX_DROP_PCT}% vs live ($BUILD_COUNT < $MIN_ALLOWED)")
        fi
    fi
    if [ -s "$FAIL_LOG" ]; then
        ERRORS+=("last import had failed files (see $FAIL_LOG)")
    fi
    if [ "$ALLOW_SCHEMA_DIFF" != "true" ] && [ "$(schema_hash "$LIVE_CORE")" != "$(schema_hash "$BUILD_CORE")" ]; then
        ERRORS+=("schema fields differ between $LIVE_CORE and $BUILD_CORE (sync conf, or ALLOW_SCHEMA_DIFF=true for v2 cutover)")
    fi

    if [ "${#ERRORS[@]}" -gt 0 ]; then
        for e in "${ERRORS[@]}"; do echo "CHECK FAILED: $e"; done
        if [ "$FORCE" != "true" ]; then
            echo "ERROR: not swapping (set FORCE=true to override)"
            exit 1
        fi
        echo "WARNING: FORCE=true, swapping anyway"
    fi

    if [ "$DRY_RUN" = "true" ]; then
        echo "[DRY RUN] Checks passed; would SWAP $LIVE_CORE <-> $BUILD_CORE"
        echo "[DRY RUN] Set DRY_RUN=false to actually swap"
        exit 0
    fi

    echo "Swapping $LIVE_CORE <-> $BUILD_CORE..."
    curl -s -f "${SOLR_HOST}/solr/admin/cores?action=SWAP&core=${LIVE_CORE}&other=${BUILD_CORE}" > /dev/null

    echo "Done. $LIVE_CORE=$(core_count "$LIVE_CORE") (new), $BUILD_CORE=$(core_count "$BUILD_CORE") (previous, kept for rollback)"
    echo "Rollback: curl \"${SOLR_HOST}/solr/admin/cores?action=SWAP&core=${LIVE_CORE}&other=${BUILD_CORE}\""
fi
