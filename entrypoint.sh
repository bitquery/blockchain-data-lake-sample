#!/bin/sh
#
# Boot SeaweedFS with the S3 endpoint, then load every bundled sample object so
# the lake is ready the moment the container starts. No external upload.
#
# Two kinds of object are seeded, both under the same per-chain prefix layout
# the production lake uses:
#
#   s3://archive/base/blocks/<block>.block.lz4              raw block, stream it
#   s3://archive/tron/blocks/<block>.block.lz4              raw block, stream it
#   s3://archive/ethereum/dex_trades/<range>.parquet        dump table, query it
#
# Blocks are Protobuf + LZ4 and are read with stream.py. Parquet dumps are the
# flattened tables and are registered into Iceberg by iceberg_query.py.

DATADIR=/data
BUCKET=archive
EP="--endpoint-url http://localhost:8333"

export AWS_ACCESS_KEY_ID="${DATA_LAKE_ACCESS_KEY:-admin}"
export AWS_SECRET_ACCESS_KEY="${DATA_LAKE_SECRET_KEY:-secret}"
export AWS_DEFAULT_REGION="${DATA_LAKE_REGION:-us-east-1}"

# Map a block filename to its chain prefix from the block-hash field.
# Filenames are <number>_<hash>_<...>.block.lz4; EVM hashes start with 0x,
# Tron hashes are bare hex (leading zeros), so the hash tells us the chain.
chain_for() {
  case "$(basename "$1" | cut -d_ -f2)" in
    0x*) echo base ;;
    *)   echo tron ;;
  esac
}

# start SeaweedFS (master + volume + filer + S3) in the background
weed server -dir="$DATADIR" -s3 -s3.port=8333 -ip.bind=0.0.0.0 &
SERVER_PID=$!

# wait for the S3 endpoint to answer before seeding. Any HTTP response means
# the server is up: the S3 root returns 403 (auth required), not 200, so we
# must NOT use curl -f here (that would treat 403 as a failure and hang).
echo "[seed] waiting for S3 endpoint on :8333 ..."
i=0
until curl -s -o /dev/null http://localhost:8333; do
  i=$((i + 1))
  if [ "$i" -gt 60 ]; then
    echo "[seed] S3 endpoint did not come up in time" >&2
    break
  fi
  sleep 1
done

# create the bucket and upload every baked-in object (idempotent; safe on restart)
aws $EP s3 mb "s3://$BUCKET" 2>/dev/null
LOADED=0
for BLOCK in /seed/*.block.lz4; do
  [ -e "$BLOCK" ] || continue
  KEY="$(chain_for "$BLOCK")/blocks/$(basename "$BLOCK")"
  if aws $EP s3 cp "$BLOCK" "s3://$BUCKET/$KEY"; then
    echo "[seed] loaded s3://$BUCKET/$KEY"
    LOADED=$((LOADED + 1))
  else
    echo "[seed] failed to load $BLOCK" >&2
  fi
done

# Parquet dumps keep the directory layout they were baked in with, so
# /seed-parquet/ethereum/dex_trades/x.parquet lands at ethereum/dex_trades/x.parquet
PARQUET=0
if [ -d /seed-parquet ]; then
  if aws $EP s3 cp --recursive /seed-parquet "s3://$BUCKET/"; then
    PARQUET=$(find /seed-parquet -name '*.parquet' | wc -l | tr -d ' ')
    echo "[seed] loaded $PARQUET Parquet file(s) under s3://$BUCKET/"
  else
    echo "[seed] failed to load Parquet dumps" >&2
  fi
fi

if [ "$LOADED" -eq 0 ] && [ "$PARQUET" -eq 0 ]; then
  echo "[seed] nothing loaded; the lake is up but empty" >&2
else
  echo "[seed] ready. $LOADED block(s) and $PARQUET Parquet file(s) loaded."
  echo "[seed] stream a block with:"
  echo "  python stream.py --bucket $BUCKET --key <chain>/blocks/<block>.block.lz4 --decode --chain <chain>"
  echo "[seed] query the Parquet dumps as an Iceberg table with:"
  echo "  python iceberg_query.py --register --examples"
fi

# keep SeaweedFS in the foreground so the container stays alive
wait "$SERVER_PID"
