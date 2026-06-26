#!/bin/sh
#
# Boot SeaweedFS with the S3 endpoint, then load the bundled sample block so the
# lake is ready to stream the moment the container starts. No external upload.
#
# Result: s3://archive/base/blocks/<block>.block.lz4 is readable at :8333.

DATADIR=/data
BUCKET=archive
EP="--endpoint-url http://localhost:8333"

export AWS_ACCESS_KEY_ID="${DATA_LAKE_ACCESS_KEY:-admin}"
export AWS_SECRET_ACCESS_KEY="${DATA_LAKE_SECRET_KEY:-secret}"
export AWS_DEFAULT_REGION="${DATA_LAKE_REGION:-us-east-1}"

# the baked-in sample block (resolve the actual filename)
BLOCK=$(ls /seed/*.block.lz4 2>/dev/null | head -1)
KEY="base/blocks/$(basename "$BLOCK")"

# start SeaweedFS (master + volume + filer + S3) in the background
weed server -dir="$DATADIR" -s3 -s3.port=8333 -ip.bind=0.0.0.0 &
SERVER_PID=$!

# wait for the S3 endpoint to answer before seeding
echo "[seed] waiting for S3 endpoint on :8333 ..."
i=0
until curl -sf http://localhost:8333 >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -gt 60 ]; then
    echo "[seed] S3 endpoint did not come up in time" >&2
    break
  fi
  sleep 1
done

# create the bucket and upload the block (idempotent; safe on restart)
if [ -n "$BLOCK" ]; then
  aws $EP s3 mb "s3://$BUCKET" 2>/dev/null
  if aws $EP s3 cp "$BLOCK" "s3://$BUCKET/$KEY"; then
    echo "[seed] loaded s3://$BUCKET/$KEY"
  else
    echo "[seed] failed to load block; the lake is up but empty" >&2
  fi
else
  echo "[seed] no .block.lz4 found in /seed; the lake is up but empty" >&2
fi

echo "[seed] ready. stream with:"
echo "  python stream.py --bucket $BUCKET --key \"$KEY\" --decode"

# keep SeaweedFS in the foreground so the container stays alive
wait "$SERVER_PID"
