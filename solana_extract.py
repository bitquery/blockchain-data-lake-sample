#!/usr/bin/env python3
"""
Extract transfers from a local Solana block file.

Reading a block from the lake works the same as for any chain (see stream.py);
this script starts from the .block.lz4 bytes and shows the Solana parsing step.

Usage:
    # all transfers, print a breakdown and the first 3
    python solana_extract.py sample/solana/000457030560_....block.lz4

    # only one token mint, e.g. USDC
    python solana_extract.py sample/solana/000457030560_....block.lz4 EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
"""

import sys, collections, lz4.frame
from solana.block_message_pb2 import BlockMessage
from solana_transfers import transfers_from_block

path = sys.argv[1]
mint = sys.argv[2] if len(sys.argv) > 2 else None

block = BlockMessage()
block.ParseFromString(lz4.frame.decompress(open(path, "rb").read()))

transfers = transfers_from_block(block)
counts = collections.Counter(t["type"] for t in transfers)
print(f"slot {block.Header.Slot}: {len(transfers)} transfers  {dict(counts)}\n")

if mint:
    transfers = [t for t in transfers if t["mint"] == mint]
for t in transfers[:3]:
    human = t["amount"] / 10 ** t["decimals"]
    print(f"[{t['type']}] {t['mint'] or 'SOL'}  {t['from']} -> {t['to']}  amount {t['amount']} ({human:g})")
