# Bitquery Blockchain Data Lake: Sample and Streaming Example

This repo shows what a block in the [Bitquery Blockchain Data Lake](https://docs.bitquery.io/docs/cloud/) looks like and how to stream one from the lake over a standard S3 interface.

## What's here

```
.
├── stream.py           # stream a block from the lake; throughput, decode, tx JSON, transfers
├── transfers.py        # transfers_from_block(): native, ERC-20, ERC-721 extraction
├── requirements.txt
├── Dockerfile          # builds the self-seeding demo lake image
├── entrypoint.sh       # boots SeaweedFS and loads the sample block
└── stream.log          # full sample output from the command below
```

### The sample block

The demo image ships a real **Base** block (Chain ID 8453), block number **46,600,927**, so you can see the exact format the lake serves:

- A single block stored as a **Protobuf** message, **LZ4-compressed**.
- About **3.2 MB** compressed and **11.6 MB** decoded.
- Contains the block header, **169 transactions**, **1,180 logs**, plus full receipts and traces.

The block schema is published at [bitquery/streaming_protobuf](https://github.com/bitquery/streaming_protobuf), and the Python bindings come from `bitquery-pb2-kafka-package`. It is the same schema Bitquery uses for its Kafka streams, so one decoder works for both.

### Sample output

After pointing the script at the demo lake (see [Stream a block](#stream-a-block)), this command streams the block, prints throughput, decodes it, and dumps the first three transactions as JSON straight from the pb2 schema:

```bash
python3 stream.py --bucket archive --key "$KEY" --tx 3
```

```
  streamed     3.24 MB  in   0.0s  ->    203.3 MB/s (1.63 Gbps)
  reads: 1, object size: 3.24 MB

  decoded 11.55 MB (evm):
  number     : 46,600,927
  hash       : 0x0133403c4fe53c434b1d2a1686d339eebd4e8e7f50ab52ab84cd68029e82e955
  timestamp  : 1779991201
  gas used   : 50,521,537
  transactions: 169
  logs        : 1,180

  first 3 transaction(s) as JSON:

--- transaction #0 ---
{
  "TransactionHeader": {
    "Hash": "w9VR+y41FcMsk8LvKHYlRRHgdCfg3kkd/lWd1K8HvS4=",
    "Gas": "1000000",
    "Data": "Pba+KwAACN0AEBwSAAAAAAAAAAQAAAAAahiCYwAAAAABgHPlAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA5HOxsAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPzQHYlB/RvebPz97nUBKWDMsXMW3xtFk5B0Vv8WHJWmDXIiAAAAAAAAAAAAAAAAUFD2mpeG8IFQkjTxp/RoS15bdskAAAAAAAAAAAAAAAAAlA==",
    "Protected": true,
    "Type": 126,
    "To": "QgAAAAAAAAAAAAAAAAAAAAAAABU=",
    "From": "3q3erd6t3q3erd6t3q3erd6tAAE=",
    "ToCode": {
      "Hash": "H5WGVKsGoVKZPnoK57bbsNSxkmXMkze4eJ/hNTvZ3DU=",
      "Size": 2055
    },
    "IsSystemTx": false,
    "SourceHash": "QdzORbzbW87xDkjNIAiUs6c+ERcQyK3Mpqs4tbhw2GM=",
    "Time": "1779991201000000000"
  },
  "Signature": {},
  "Receipt": {
    "ReceiptHeader": {
      "GasUsed": "46218",
      "Type": 126,
      "CumulativeGasUsed": "46218",
      "Status": "1"
    }
  },
  "Trace": { "... full call trace and opcode capture states ..." }
}

--- transaction #1 ---
{
  "TransactionHeader": {
    "Index": "1",
    "Hash": "y+Zdue1Xv1ewK5fb3WZylm1rMqkHzdtRHUcxmjX1asM=",
    "Gas": "100000",
    "Type": 2,
    "To": "QIdhDPfPAYPmJs87yEFSpCc3S6M=",
    "From": "guFZ1j5YUGfp+jpLun2ZL7xmd1E="
  },
  "Receipt": {
    "ReceiptHeader": {
      "GasUsed": "46371",
      "CumulativeGasUsed": "92589",
      "Status": "1"
    },
    "Logs": [
      {
        "LogHeader": {
          "Address": "QIdhDPfPAYPmJs87yEFSpCc3S6M=",
          "Data": "//////////////////////////////////////////8="
        },
        "Topics": [
          { "Hash": "jFvh5evsfVvRT3FCfR6E890DFMD3sikeWyAKyMfDuSU=" },
          { "Index": "1", "Hash": "AAAAAAAAAAAAAAAAguFZ1j5YUGfp+jpLun2ZL7xmd1E=" },
          { "Index": "2", "Hash": "AAAAAAAAAAAAAAAA2LqdGpn8IfDsok6bhXN8KKGUpOI=" }
        ]
      }
    ]
  },
  "Trace": { "... full execution trace ..." }
}

--- transaction #2 ---
{ "... same pb2 fields: header, signature, receipt, logs, trace ..." }
```

Each transaction JSON includes the full pb2 structure: header, signature, receipt (with logs), and trace. The excerpt above trims bloom filters, long `Data` fields, and trace internals for readability. See `stream.log` for the complete run (~23k lines).

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

Stream, decode, and print the first three transactions:

```bash
python3 stream.py --bucket archive --key "$KEY" --tx 3
```

For a block summary only (no transaction JSON):

```bash
python3 stream.py --bucket archive --key "$KEY" --decode
```

Keep streaming for a fixed window to see a steady rate:

```bash
python3 stream.py --bucket archive --key "$KEY" --duration 15
```

Run several readers in parallel to see aggregate throughput scale:

```bash
python3 stream.py --bucket archive --key "$KEY" --duration 15 --concurrency 16
```

Extract transfers from the block (native, ERC-20, ERC-721):

```bash
python3 stream.py --bucket archive --key "$KEY" --transfers
python3 stream.py --bucket archive --key "$KEY" --transfers 5 --token 0x4200000000000000000000000000000000000006
```

```
  469 transfers  {'erc20': 422, 'native': 28, 'erc721': 19}

  [erc20] 0x4200000000000000000000000000000000000006  0x498581ff... -> 0xbf4195ab...  amount 3390958905493657
```

The parser is `transfers_from_block()` in `transfers.py`. See [Extract transfers from a block](https://docs.bitquery.io/docs/data-lake/extract-transfers/) for the full walkthrough.

## Options

| Flag | Default | Purpose |
| --- | --- | --- |
| `--endpoint` | `DATA_LAKE_ENDPOINT` env | S3 endpoint of the data lake |
| `--bucket` | (required) | Bucket name |
| `--key` | (required) | Object key of the `.block.lz4` file |
| `--region` | `DATA_LAKE_REGION` env, else `us-east-1` | S3 region |
| `--chain` | `evm` | Schema to decode with: `evm`, `solana`, `tron`, `utxo` |
| `--decode` | off | Decode the block after streaming and print a summary |
| `--tx N` | `0` | After streaming, decode and print the first N transactions as JSON (full pb2 fields). Implies decode. |
| `--json` | off | After streaming, decode and print the full block as JSON (large). Implies decode. |
| `--transfers N` | `0` | After streaming, extract transfers (native, ERC-20, ERC-721) and print the first N (`-1` for all). EVM only. Implies decode. |
| `--token 0x...` | none | With `--transfers`, only show this token contract |
| `--duration` | `0` | Keep streaming for N seconds |
| `--concurrency` | `1` | Number of parallel readers; greater than 1 runs fan-out mode |

## Decode the local sample without streaming

The demo image bakes the block into `/seed/`. You can also decode a downloaded `.block.lz4` directly:

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
