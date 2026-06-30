#!/usr/bin/env python3
"""
Stream a block from the Bitquery Blockchain Data Lake.

The lake exposes blocks over a standard S3 interface. This script reads one
block object, shows live throughput, and (optionally) decodes it with the
Bitquery protobuf schema. A --concurrency mode runs several readers at once to
show how aggregate bandwidth scales across the cluster.

Bitquery hosts the lake and uploads the blocks. You only need read access:
an endpoint, a bucket, an object key, and your data lake credentials.

Usage:
    pip install -r requirements.txt

    # run the demo lake first:  docker run -p 8333:8333 marketingbitquery/datalake-demo
    export DATA_LAKE_ENDPOINT=http://localhost:8333
    export DATA_LAKE_ACCESS_KEY=admin
    export DATA_LAKE_SECRET_KEY=secret

    # stream once and print throughput
    python stream.py --bucket archive --key base/blocks/000046600927_....block.lz4

    # stream and decode the block
    python stream.py --bucket archive --key <key> --decode

    # keep streaming for 15s to see a steady rate
    python stream.py --bucket archive --key <key> --duration 15

    # run 16 parallel readers and report aggregate throughput
    python stream.py --bucket archive --key <key> --duration 15 --concurrency 16
"""

import argparse
import os
import sys
import threading
import time

import boto3
import lz4.frame
from google.protobuf.json_format import MessageToJson

from transfers import transfers_from_block

CHUNK = 64 * 1024  # 64 KB read chunks


def make_client(endpoint, region):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=os.environ.get("DATA_LAKE_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("DATA_LAKE_SECRET_KEY"),
    )


def stream_once(client, bucket, key):
    """Stream one object, return (raw_bytes, elapsed_seconds)."""
    start = time.time()
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    buf = bytearray()
    for chunk in iter(lambda: body.read(CHUNK), b""):
        buf.extend(chunk)
    return bytes(buf), time.time() - start


def human_mbps(num_bytes, seconds):
    if seconds <= 0:
        return 0.0
    return (num_bytes / (1024 * 1024)) / seconds


def decode_block(raw, chain="evm"):
    """LZ4-decompress and parse a block with the Bitquery protobuf schema."""
    payload = lz4.frame.decompress(raw)
    module = __import__(f"{chain}.block_message_pb2", fromlist=["BlockMessage"])
    block = module.BlockMessage()
    block.ParseFromString(payload)
    return block, len(payload)


def to_int(b):
    return int.from_bytes(b, "big") if isinstance(b, (bytes, bytearray)) else b


def summarize_evm(block):
    h = block.Header
    logs = sum(
        len(tx.Receipt.Logs) for tx in block.Transactions if tx.HasField("Receipt")
    )
    return (
        f"  number     : {to_int(h.Number):,}\n"
        f"  hash       : 0x{h.Hash.hex()}\n"
        f"  timestamp  : {to_int(h.Time)}\n"
        f"  gas used   : {to_int(h.GasUsed):,}\n"
        f"  transactions: {len(block.Transactions):,}\n"
        f"  logs        : {logs:,}"
    )


def run_single(args):
    client = make_client(args.endpoint, args.region)

    total_bytes = 0
    reads = 0
    start = time.time()
    raw = None
    deadline = start + args.duration if args.duration else None

    while True:
        raw, _ = stream_once(client, args.bucket, args.key)
        total_bytes += len(raw)
        reads += 1
        elapsed = time.time() - start
        sys.stdout.write(
            f"\r  streamed {total_bytes / (1024*1024):8.2f} MB  "
            f"in {elapsed:5.1f}s  ->  {human_mbps(total_bytes, elapsed):7.1f} MB/s "
            f"({human_mbps(total_bytes, elapsed) * 8 / 1000:.2f} Gbps)"
        )
        sys.stdout.flush()
        if deadline is None or time.time() >= deadline:
            break

    print(f"\n  reads: {reads}, object size: {len(raw) / (1024*1024):.2f} MB")

    if (args.decode or args.tx or args.json or args.transfers) and raw is not None:
        block, decoded_size = decode_block(raw, args.chain)
        print(f"\n  decoded {decoded_size / (1024*1024):.2f} MB ({args.chain}):")
        if args.chain == "evm":
            print(summarize_evm(block))
        else:
            print(f"  parsed BlockMessage with {block.ByteSize():,} bytes")

        # full structure straight from the pb2 schema
        if args.tx:
            txs = list(block.Transactions)[: args.tx]
            print(f"\n  first {len(txs)} transaction(s) as JSON:\n")
            for n, tx in enumerate(txs):
                print(f"--- transaction #{n} ---")
                print(MessageToJson(tx))
        if args.json:
            print("\n  full block as JSON:\n")
            print(MessageToJson(block))
        if args.transfers:
            print_transfers(block, token=args.token, limit=args.transfers)


def print_transfers(block, token=None, limit=10):
    """Extract transfers from a decoded block and print a breakdown."""
    transfers = transfers_from_block(block)
    if token:
        token = token.lower()
        transfers = [t for t in transfers if (t["token"] or "").lower() == token]

    counts = {}
    for t in transfers:
        counts[t["type"]] = counts.get(t["type"], 0) + 1
    print(f"\n  {len(transfers)} transfers  {counts}\n")

    shown = transfers if limit < 0 else transfers[:limit]
    for t in shown:
        if t["type"] == "erc721":
            print(f"  [{t['type']}] {t['token']}  {t['from']} -> {t['to']}  id {t['token_id']}")
        elif t["type"] == "native":
            print(f"  [{t['type']}] {t['from']} -> {t['to']}  amount {t['amount']}")
        else:
            print(f"  [{t['type']}] {t['token']}  {t['from']} -> {t['to']}  amount {t['amount']}")


def run_fanout(args):
    client = make_client(args.endpoint, args.region)
    counter = {"bytes": 0}
    lock = threading.Lock()
    stop_at = time.time() + (args.duration or 10)

    def worker():
        local = 0
        while time.time() < stop_at:
            raw, _ = stream_once(client, args.bucket, args.key)
            local += len(raw)
        with lock:
            counter["bytes"] += local

    start = time.time()
    threads = [threading.Thread(target=worker) for _ in range(args.concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    mbps = human_mbps(counter["bytes"], elapsed)
    print(
        f"  concurrency : {args.concurrency}\n"
        f"  total read  : {counter['bytes'] / (1024*1024):.2f} MB in {elapsed:.1f}s\n"
        f"  aggregate   : {mbps:.1f} MB/s  ({mbps * 8 / 1000:.2f} Gbps)\n"
        f"\n  In production the same pattern fans out across many volume servers,\n"
        f"  scaling aggregate throughput well beyond a single client."
    )


def main():
    p = argparse.ArgumentParser(description="Stream a block from the Bitquery Data Lake.")
    p.add_argument("--endpoint", default=os.environ.get("DATA_LAKE_ENDPOINT"),
                   help="S3 endpoint of the data lake (or set DATA_LAKE_ENDPOINT).")
    p.add_argument("--bucket", required=True, help="Bucket name.")
    p.add_argument("--key", required=True, help="Object key of the .block.lz4 file.")
    p.add_argument("--region", default=os.environ.get("DATA_LAKE_REGION", "us-east-1"))
    p.add_argument("--chain", default="evm", choices=["evm", "solana", "tron", "utxo"],
                   help="Schema module to decode with (default: evm).")
    p.add_argument("--decode", action="store_true", help="Decode the block after streaming.")
    p.add_argument("--tx", type=int, default=0, metavar="N",
                   help="After decoding, print the first N transactions as JSON.")
    p.add_argument("--json", action="store_true",
                   help="After decoding, print the full block as JSON (large).")
    p.add_argument("--transfers", type=int, default=0, nargs="?", const=10, metavar="N",
                   help="After decoding, extract transfers and print the first N "
                        "(default 10; -1 for all). EVM only. Implies decode.")
    p.add_argument("--token", help="With --transfers, only show this token contract.")
    p.add_argument("--duration", type=int, default=0,
                   help="Keep streaming for N seconds (single mode) or run window (fan-out).")
    p.add_argument("--concurrency", type=int, default=1,
                   help="Number of parallel readers (>1 enables fan-out mode).")
    args = p.parse_args()

    if not args.endpoint:
        p.error("no endpoint: pass --endpoint or set DATA_LAKE_ENDPOINT")

    if args.concurrency > 1:
        run_fanout(args)
    else:
        run_single(args)


if __name__ == "__main__":
    main()
