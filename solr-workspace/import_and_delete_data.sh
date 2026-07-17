#!/bin/bash
set -eo pipefail

# ===== 設定 (可透過環境變數覆寫) =====
CSV_DIR="${CSV_DIR:-/var/solr/csvs/export}"
CSV_PATTERN="${CSV_PATTERN:-*.csv}"
SOLR_HOST="${SOLR_HOST:-http://solr:8983}"
CORE="${CORE:-tbia_records}"
CUTOFF_FILE="${CUTOFF_FILE:-/bucket/tbia_cutoff.txt}"
MIN_EXPECTED_COUNT="${MIN_EXPECTED_COUNT:-1000000}"
MAX_JOBS="${MAX_JOBS:-6}"      # 控制平行寫入的線程數
DRY_RUN="${DRY_RUN:-true}"
CUTOFF="${CUTOFF:-}"           # 直接傳入時優先使用，否則讀檔
ACTION="${ACTION:-all}"        # all | import | delete
FAIL_LOG="${FAIL_LOG:-/bucket/tbia_import_failures.txt}"
PROGRESS_STEPS="${PROGRESS_STEPS:-50}"   # 進度大約更新幾次(檔案少時每個都更新)

# ===== 決定 cutoff =====
if [ -z "$CUTOFF" ]; then
    if [ ! -f "$CUTOFF_FILE" ]; then
        echo "ERROR: cutoff not provided and cutoff file not found: $CUTOFF_FILE"
        echo "       set CUTOFF=YYYY-MM-DDTHH:MM:SSZ or create $CUTOFF_FILE"
        exit 1
    fi
    CUTOFF=$(cat "$CUTOFF_FILE")
    echo "Cutoff (from file): $CUTOFF"
else
    echo "Cutoff (from env): $CUTOFF"
fi

echo "Action: $ACTION"

# ===== 1. 匯入 csv =====
if [ "$ACTION" = "import" ] || [ "$ACTION" = "all" ]; then
    cd "$CSV_DIR"
    CSV_COUNT=$(find . -maxdepth 1 -name "$CSV_PATTERN" | wc -l)
    echo "Found $CSV_COUNT csv files to import (pattern: $CSV_PATTERN)"

    if [ "$CSV_COUNT" -eq 0 ]; then
        echo "ERROR: no csv files found in $CSV_DIR"
        exit 1
    fi

    echo "Importing csv files using $MAX_JOBS concurrent jobs..."

    : > "$FAIL_LOG"                         # 清空失敗清單
    export SOLR_HOST CORE FAIL_LOG

    # 進度間隔依檔案數自動調整，至少為 1
    STEP=$(( CSV_COUNT / PROGRESS_STEPS ))
    if [ "$STEP" -lt 1 ]; then STEP=1; fi

    # 失敗的檔案略過並記錄到 $FAIL_LOG，單檔失敗不會中止整批
    # 進度改用 while read 即時輸出(避開 mawk 導向檔案時 fflush 不生效的問題)
    find . -maxdepth 1 -name "$CSV_PATTERN" -print0 \
        | xargs -0 -P "$MAX_JOBS" -I {} bash -c '
            f="$1"
            if curl -s -o /dev/null -f -X POST -H "Content-Type: text/csv" \
                 --data-binary @"$f" "${SOLR_HOST}/solr/${CORE}/update"; then
                printf "OK\n"
            else
                printf "%s\n" "$f" >> "$FAIL_LOG"   # O_APPEND 短行為原子寫入，平行安全
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

    # 匯入失敗回報
    FAIL_TOTAL=$(wc -l < "$FAIL_LOG" | tr -d ' ')
    if [ "$FAIL_TOTAL" -gt 0 ]; then
        echo "WARNING: $FAIL_TOTAL 個檔案匯入失敗，清單見 $FAIL_LOG"
    fi

    # ===== 2. Commit =====
    echo "Committing..."
    curl -s "${SOLR_HOST}/solr/${CORE}/update?commit=true" > /dev/null

    # ===== 3. Sanity check =====
    COUNT=$(curl -s "${SOLR_HOST}/solr/${CORE}/select?q=*:*&rows=0" \
        | grep -oP '"numFound":\s*\K\d+')
    echo "After import: $COUNT records"

    if [ -z "$COUNT" ] || [ "$COUNT" -lt "$MIN_EXPECTED_COUNT" ]; then
        echo "ERROR: count too low ($COUNT < $MIN_EXPECTED_COUNT), aborting delete"
        exit 1
    fi
fi

# ===== 4. 刪除 stale records =====
if [ "$ACTION" = "delete" ] || [ "$ACTION" = "all" ]; then
    # 若是單獨 delete，COUNT 還沒被設定，補抓一次
    if [ -z "${COUNT:-}" ]; then
        COUNT=$(curl -s "${SOLR_HOST}/solr/${CORE}/select?q=*:*&rows=0" \
            | grep -oP '"numFound":\s*\K\d+')
        echo "Current count: $COUNT"

        if [ -z "$COUNT" ] || [ "$COUNT" -lt "$MIN_EXPECTED_COUNT" ]; then
            echo "ERROR: count too low ($COUNT < $MIN_EXPECTED_COUNT), aborting delete"
            exit 1
        fi
    fi

    # URL encode 的 query (用於 GET 預覽)
    PREVIEW_QUERY="modified:%5B*+TO+${CUTOFF}%5D"
    PREVIEW_COUNT=$(curl -s "${SOLR_HOST}/solr/${CORE}/select?q=${PREVIEW_QUERY}&rows=0" \
        | grep -oP '"numFound":\s*\K\d+')

    if [ "$DRY_RUN" = "true" ]; then
        echo "[DRY RUN] Would delete $PREVIEW_COUNT records (modified <= $CUTOFF)"
        echo "[DRY RUN] Set DRY_RUN=false to actually delete"
        exit 0
    fi

    echo "Deleting $PREVIEW_COUNT records (modified <= $CUTOFF)..."
    curl -s "${SOLR_HOST}/solr/${CORE}/update/?commit=true" \
        -H "Content-Type: text/xml" \
        --data-binary "<delete><query>modified:[* TO ${CUTOFF}]</query></delete>" > /dev/null
    echo ""

    # ===== 5. 確認結果 =====
    AFTER=$(curl -s "${SOLR_HOST}/solr/${CORE}/select?q=*:*&rows=0" \
        | grep -oP '"numFound":\s*\K\d+')
    DELETED=$((COUNT - AFTER))
    echo "Done. Final count: $AFTER (deleted: $DELETED)"
fi