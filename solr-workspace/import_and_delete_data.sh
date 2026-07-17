#!/bin/bash
set -eo pipefail

# ===== 設定 (可透過環境變數覆寫) =====
CSV_DIR="${CSV_DIR:-/var/solr/csvs/export}"
CSV_PATTERN="${CSV_PATTERN:-*.csv}"
SOLR_HOST="${SOLR_HOST:-http://solr:8983}"
CORE="${CORE:-tbia_records}"
CUTOFF_FILE="${CUTOFF_FILE:-/bucket/tbia_cutoff.txt}"
MIN_EXPECTED_COUNT="${MIN_EXPECTED_COUNT:-1000000}"
MAX_JOBS="${MAX_JOBS:-4}"      # 新增：控制平行寫入的線程數 (建議為 CPU 核心數的 1~2 倍)
DRY_RUN="${DRY_RUN:-true}"
CUTOFF="${CUTOFF:-}"           # 直接傳入時優先使用，否則讀檔
ACTION="${ACTION:-all}"        # all | import | delete

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
    
    # 速度優化核心：移除 post，使用 xargs -P 搭配 curl 進行平行多線程上傳
    # 加上 -f (fail-fast) 確保遇到 HTTP 錯誤時 xargs 會捕捉到並報錯
    find . -maxdepth 1 -name "$CSV_PATTERN" -print0 \
        | xargs -0 -n 1 -P "$MAX_JOBS" -I {} \
        curl -s -f -X POST -H 'Content-Type: text/csv' \
        --data-binary @{} "${SOLR_HOST}/solr/${CORE}/update"

    # ===== 2. Commit =====
    echo "Committing..."
    # 錯誤修復：改用 API 進行 commit
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
    if [ -z "${COUNT:-}" ]; then
        COUNT=$(curl -s "${SOLR_HOST}/solr/${CORE}/select?q=*:*&rows=0" \
            | grep -oP '"numFound":\s*\K\d+')
        echo "Current count: $COUNT"

        if [ -z "$COUNT" ] || [ "$COUNT" -lt "$MIN_EXPECTED_COUNT" ]; then
            echo "ERROR: count too low ($COUNT < $MIN_EXPECTED_COUNT), aborting delete"
            exit 1
        fi
    fi

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