#!/bin/bash
set -eo pipefail

# ===== 設定 (可透過環境變數覆寫) =====
CSV_DIR="${CSV_DIR:-/var/solr/csvs/export}"
SOLR_HOST="${SOLR_HOST:-http://solr:8983}"
CORE="${CORE:-tbia_records}"
CUTOFF_FILE="${CUTOFF_FILE:-/bucket/tbia_cutoff.txt}"
MIN_EXPECTED_COUNT="${MIN_EXPECTED_COUNT:-1000000}"
BATCH_SIZE="${BATCH_SIZE:-100}"
DRY_RUN="${DRY_RUN:-true}"
CUTOFF="${CUTOFF:-}"   # 直接傳入時優先使用，否則讀檔

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

# ===== 1. 匯入 csv =====
cd "$CSV_DIR"
CSV_COUNT=$(find . -maxdepth 1 -name '*.csv' | wc -l)
echo "Found $CSV_COUNT csv files to import"

if [ "$CSV_COUNT" -eq 0 ]; then
    echo "ERROR: no csv files found in $CSV_DIR"
    exit 1
fi

echo "Importing csv files (batch size: $BATCH_SIZE)..."
find . -maxdepth 1 -name '*.csv' -print0 \
    | xargs -0 -n "$BATCH_SIZE" post -c "$CORE" -commit no

# ===== 2. Commit =====
echo "Committing..."
post -c "$CORE" -d '<commit/>'

# ===== 3. Sanity check =====
COUNT=$(curl -s "${SOLR_HOST}/solr/${CORE}/select?q=*:*&rows=0" \
    | grep -oP '"numFound":\s*\K\d+')
echo "After import: $COUNT records"

if [ -z "$COUNT" ] || [ "$COUNT" -lt "$MIN_EXPECTED_COUNT" ]; then
    echo "ERROR: count too low ($COUNT < $MIN_EXPECTED_COUNT), aborting delete"
    exit 1
fi

# ===== 4. 刪除 stale records =====
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
    --data-binary "<delete><query>modified:[* TO ${CUTOFF}]</query></delete>"
echo ""

# ===== 5. 確認結果 =====
AFTER=$(curl -s "${SOLR_HOST}/solr/${CORE}/select?q=*:*&rows=0" \
    | grep -oP '"numFound":\s*\K\d+')
DELETED=$((COUNT - AFTER))
echo "Done. Final count: $AFTER (deleted: $DELETED)"