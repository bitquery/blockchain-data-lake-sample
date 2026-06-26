# Bitquery Blockchain Data Lake: Sample and Streaming Example

This repo shows what a block in the [Bitquery Blockchain Data Lake](https://docs.bitquery.io/docs/cloud/) looks like and how to stream one from the lake over a standard S3 interface.

## What's here

```
.
├── 000046600927_0x0133…fd44.block.lz4   # a real block, exactly as stored in the lake
├── stream.py                            # stream a block from the lake, with throughput + decode
└── requirements.txt
```

### The sample block

The `.block.lz4` file is a real **Base** block (Chain ID 8453), block number **46,600,927**, kept here as a reference so you can see the exact format the lake serves:

- A single block stored as a **Protobuf** message, **LZ4-compressed**.
- About **3.4 MB** compressed and **12.1 MB** decoded.
- Contains the block header, **169 transactions**, **1,180 logs**, plus full receipts and traces.

The block schema is published at [bitquery/streaming_protobuf](https://github.com/bitquery/streaming_protobuf), and the Python bindings come from `bitquery-pb2-kafka-package`. It is the same schema Bitquery uses for its Kafka streams, so one decoder works for both.

## How blocks are stored

The data lake holds the complete archive of each chain, from genesis to the current tip, as one `.block.lz4` file per block. Files are named by block number and hash:

```
<block_number>_<block_hash>_<...>.block.lz4
```

Bitquery hosts the lake and writes the blocks. You read them over an S3 interface, which means any standard S3 client works (`aws s3`, boto3, s3fs).

## Stream a block

```bash
pip install -r requirements.txt

KEY="base/blocks/000046600927_0x0133403c4fe53c434b1d2a1686d339eebd4e8e7f50ab52ab84cd68029e82e955_49e9339dd61bdb91320044378bff935efd925d868ca257ef8c3bc42177f9fd44.block.lz4"
```

Run the demo lake. We publish a ready-to-run image that starts a SeaweedFS lake with the sample block already loaded. You do not upload anything.

```bash
docker run -p 8333:8333 marketingbitquery/datalake-demo
```

This gives you a lake at `http://localhost:8333`. Point the script at it:

```bash
export DATA_LAKE_ENDPOINT=http://localhost:8333
export DATA_LAKE_ACCESS_KEY=admin
export DATA_LAKE_SECRET_KEY=secret
```

Stream and decode the block:

```bash
python stream.py --bucket archive --key "$KEY" --decode
```

```
  decoded 12.12 MB (evm):
  number     : 46,600,927
  hash       : 0x0133403c4fe53c434b1d2a1686d339eebd4e8e7f50ab52ab84cd68029e82e955
  timestamp  : 1779991201
  gas used   : 50,521,537
  transactions: 169
  logs        : 1,180
```

Keep streaming for a fixed window to see a steady rate:

```bash
python stream.py --bucket archive --key "$KEY" --duration 15
```

Run several readers in parallel to see aggregate throughput scale:

```bash
python stream.py --bucket archive --key "$KEY" --duration 15 --concurrency 16
```

## Options

| Flag | Default | Purpose |
| --- | --- | --- |
| `--endpoint` | `DATA_LAKE_ENDPOINT` env | S3 endpoint of the data lake |
| `--bucket` | (required) | Bucket name |
| `--key` | (required) | Object key of the `.block.lz4` file |
| `--region` | `DATA_LAKE_REGION` env, else `us-east-1` | S3 region |
| `--chain` | `evm` | Schema to decode with: `evm`, `solana`, `tron`, `utxo` |
| `--decode` | off | Decode the block after streaming and print a summary |
| `--tx N` | `0` | After decoding, print the first N transactions as JSON (full pb2 fields) |
| `--json` | off | After decoding, print the full block as JSON (large) |
| `--duration` | `0` | Keep streaming for N seconds |
| `--concurrency` | `1` | Number of parallel readers; greater than 1 runs fan-out mode |

## Decode the local sample without streaming

If you only want to see the format, you can decode the bundled file directly:

```python
import lz4.frame
from evm.block_message_pb2 import BlockMessage

raw = open("000046600927_0x0133403c4fe53c434b1d2a1686d339eebd4e8e7f50ab52ab84cd68029e82e955_49e9339dd61bdb91320044378bff935efd925d868ca257ef8c3bc42177f9fd44.block.lz4", "rb").read()
block = BlockMessage()
block.ParseFromString(lz4.frame.decompress(raw))
print(int.from_bytes(block.Header.Number, "big"), len(block.Transactions))
```

## Links

- [Data Lake documentation](https://docs.bitquery.io/docs/data-lake/)
- [Demo image on Docker Hub](https://hub.docker.com/r/marketingbitquery/datalake-demo)
- [Block schema](https://github.com/bitquery/streaming_protobuf)
- [SeaweedFS](https://github.com/seaweedfs/seaweedfs)
