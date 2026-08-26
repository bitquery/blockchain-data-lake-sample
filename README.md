# Bitquery Blockchain Data Lake: Sample and Streaming Example

This repo shows what the [Bitquery Blockchain Data Lake](https://docs.bitquery.io/docs/cloud/) serves and how to read it over a standard S3 interface.

The lake holds two kinds of object, for two different jobs:

| Object | Format | Read with | Good for |
| --- | --- | --- | --- |
| `<block>.block.lz4` | Protobuf + LZ4, one file per block | `stream.py` | Full fidelity: receipts, logs, traces. Replay, indexing, streaming. |
| `<range>.parquet` | Parquet, flattened dump tables | `iceberg_query.py` | SQL analytics. Registered into Iceberg, read by Spark / Trino / DuckDB / Athena. |

Raw blocks are not queryable by an Iceberg engine — Iceberg reads columnar files (Parquet/ORC/Avro) plus table metadata, and a protobuf blob is neither. The Parquet dumps are the analytics surface, and `iceberg_query.py` turns them into a real Iceberg table without moving any data.

## What's here

```
.
├── stream.py           # stream a block from the lake; throughput, decode, tx JSON, transfers
├── transfers.py        # transfers_from_block(): native, ERC-20, ERC-721 extraction
├── iceberg_query.py    # register the lake's Parquet dumps as an Iceberg table and run SQL
├── requirements.txt
├── Dockerfile          # builds the self-seeding demo lake image
├── entrypoint.sh       # boots SeaweedFS and loads the sample blocks and Parquet dumps
├── sample/             # sample files baked into the demo image
│   ├── *.block.lz4     #   Base + Tron blocks
│   └── parquet/        #   ethereum/dex_trades/ dump
└── logs/
    ├── base-block-stream.log     # Base block: stream + decode + first 3 tx as JSON
    ├── base-block-transfers.log  # Base block: --transfers run
    └── tron-block-stream.log     # Tron block: stream + decode + first 3 tx as JSON
```

### The sample blocks

The demo image ships two real blocks from two different chains, so you can see the exact format the lake serves and how the same pipeline handles different schemas. Each block is stored under a per-chain prefix — `base/blocks/` and `tron/blocks/`.

**Base** (EVM, Chain ID 8453), block number **46,600,927**:

- A single block stored as a **Protobuf** message, **LZ4-compressed**.
- About **3.2 MB** compressed and **11.6 MB** decoded.
- Contains the block header, **169 transactions**, **1,180 logs**, plus full receipts and traces.
- Decode with `--chain evm` (the default).

**Tron** (mainnet), block number **71,523,078** (2025-04-21T08:28:48Z):

- Same **Protobuf** + **LZ4** format, decoded with the Tron schema.
- About **265 KB** compressed and **2.2 MB** decoded.
- Contains the block header, **394 transactions**, plus per-transaction results, receipts, and contracts.
- Decode with `--chain tron`.

**Ethereum dex_trades** (Parquet dump), blocks **24,053,500 - 24,053,549**:

- One **Parquet** file covering a 50-block range: `ethereum/dex_trades/24053500_24053549.parquet`.
- About **376 KB**, **1,773 trades**, **53 columns** — the flattened DEX trade table, not raw blocks.
- Columns are the dump schema: `Block_Number`, `Trade_Buy_*`, `Trade_Sell_*`, `Trade_Dex_*`, `Transaction_*`.
- Same schema as the cloud dumps in [blockchain-cloud-data-dump-sample](https://github.com/bitquery/blockchain-cloud-data-dump-sample), served from `s3://bitquery-blockchain-dataset/ethereum/dex_trades/`.

**Ethereum transfers** (Parquet dump), same blocks **24,053,500 - 24,053,549**:

- One **Parquet** file: `ethereum/transfers/24053500_24053549.parquet`.
- About **1.3 MB**, **26,563 transfers**, **24 columns** — native, ERC-20, and internal (`call`) transfers flattened into one table.
- Columns are the dump schema: `Block_Number`, `Transfer_Amount`, `Transfer_AmountInUSD`, `Transfer_Sender`, `Transfer_Receiver`, `Transfer_Currency_*`, `Transfer_Type`.
- Served from `s3://bitquery-blockchain-dataset/ethereum/transfers/`.

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

Each transaction JSON includes the full pb2 structure: header, signature, receipt (with logs), and trace. The excerpt above trims bloom filters, long `Data` fields, and trace internals for readability. See `logs/base-block-stream.log` for the complete run (~23k lines), and `logs/tron-block-stream.log` for the equivalent Tron run.

## How the lake is laid out

The data lake holds the complete archive of each chain, from genesis to the current tip, as one `.block.lz4` file per block. Parquet dumps live beside them under a per-table prefix, each file covering a block range:

```
archive/
├── base/blocks/<block_number>_<block_hash>_<...>.block.lz4
├── tron/blocks/<block_number>_<block_hash>_<...>.block.lz4
└── ethereum/
    ├── dex_trades/<first_block>_<last_block>.parquet
    └── transfers/<first_block>_<last_block>.parquet
```

Bitquery hosts the lake and writes both. You read them over an S3 interface, which means any standard S3 client works (`aws s3`, boto3, s3fs) — and, for the Parquet side, any Iceberg engine once the files are registered.

## Stream a block

```bash
pip install -r requirements.txt

# Base (EVM) block
KEY="base/blocks/000046600927_0x0133403c4fe53c434b1d2a1686d339eebd4e8e7f50ab52ab84cd68029e82e955_49e9339dd61bdb91320044378bff935efd925d868ca257ef8c3bc42177f9fd44.block.lz4"

# Tron block (decode with --chain tron)
TRON_KEY="tron/blocks/000071523078_0000000004435b06e7e8e836cc1738983b576d4b5ee7a98d1d7877d58c4e1aef_0c72a82a56df49720c455ba0d8e78c8c8a835f9b89bb42e263680397660551a1.block.lz4"
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

Stream and decode the Tron block from the same lake — same command, just the Tron key and `--chain tron`:

```bash
python3 stream.py --bucket archive --key "$TRON_KEY" --chain tron --tx 3
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

## Query the Parquet dumps with an Iceberg engine

Parquet files sitting in a bucket are files, not a table. An engine pointed at them has to list objects, guess the layout, and read every file for every query. Iceberg fixes that by adding a metadata layer: schema, snapshots, manifests, and per-file column statistics.

`iceberg_query.py` builds that layer over the Parquet already in the lake. It uses PyIceberg's `add_files`, which **registers the existing S3 objects as Iceberg data files** — no rewrite, no copy, no second storage bill. The only new bytes are the small metadata tree under `s3://archive/iceberg/`.

```bash
pip install -r requirements.txt

export DATA_LAKE_ENDPOINT=http://localhost:8333
export DATA_LAKE_ACCESS_KEY=admin
export DATA_LAKE_SECRET_KEY=secret

# create the table, register the Parquet, print the schema, run example queries
python3 iceberg_query.py
```

```
  registered 1 Parquet file(s) as Iceberg table ethereum.dex_trades
    s3://archive/ethereum/dex_trades/24053500_24053549.parquet
  table metadata : s3://archive/iceberg/
  catalog        : sqlite:///iceberg_catalog.db

  scanned 1,773 rows into DuckDB

  Trades per DEX protocol
  -----------------------
  ┌─────────────────┬────────┬────────────┐
  │    protocol     │ trades │ volume_usd │
  ├─────────────────┼────────┼────────────┤
  │ uniswap_v3      │    690 │  588603.52 │
  │ uniswap_v4      │    285 │  270065.25 │
  │ uniswap_v2      │    625 │  113098.91 │
  │ curve_v1        │      5 │   41400.67 │
  │ pancake_swap_v3 │     47 │   22601.65 │
  └─────────────────┴────────┴────────────┘
```

Register only, then query separately:

```bash
python3 iceberg_query.py --register
python3 iceberg_query.py --examples
```

Run your own SQL. The Iceberg scan is handed to DuckDB as a view **named after the table** — `dex_trades` for the default table, `transfers` for the transfers table, and so on:

```bash
python3 iceberg_query.py --sql "
  SELECT Trade_Dex_ProtocolName AS protocol,
         count(*) AS trades,
         round(sum(Trade_Buy_AmountInUSD), 2) AS volume_usd
  FROM dex_trades
  GROUP BY 1
  ORDER BY volume_usd DESC"
```

The lake ships a second dump, `ethereum.transfers`. Register it and query it the same way — the view is `transfers`:

```bash
python3 iceberg_query.py --register --table ethereum.transfers \
  --key ethereum/transfers/24053500_24053549.parquet --sql "
  SELECT Transfer_Currency_Symbol AS token,
         count(*) AS transfers,
         round(sum(Transfer_AmountInUSD), 0) AS usd
  FROM transfers
  WHERE Transfer_Currency_Symbol <> ''
  GROUP BY 1
  ORDER BY usd DESC NULLS LAST
  LIMIT 10"
```

The built-in `--examples` are written for `dex_trades`; for any other table use `--sql` with its view name.

Push a filter into the Iceberg scan so non-matching files are never fetched from S3:

```bash
python3 iceberg_query.py --where "Trade_Buy_Currency_Symbol == 'WETH'" --examples
```

See what Iceberg added on top of the raw Parquet — snapshots, data files, record counts:

```bash
python3 iceberg_query.py --metadata
```

Register more than one file into the same table by repeating `--key`:

```bash
python3 iceberg_query.py --register --replace \
  --key ethereum/dex_trades/24053500_24053549.parquet \
  --key ethereum/dex_trades/24053550_24053599.parquet
```

### A note on USD amounts

The dump records the price the DEX quoted, and an illiquid or spoofed pair can quote nonsense. This 50-block sample contains two XOR trades priced at ~4e23 USD. The USD examples apply a `Trade_Buy_AmountInUSD < 1e9` bound, and the "Outlier USD amounts" example prints exactly what that bound excludes rather than hiding it. Any real analytics on this data needs a similar sanity check — an unbounded `sum(Trade_Buy_AmountInUSD)` will be wrong.

### The catalog

By default the catalog is a local SQLite file (`iceberg_catalog.db`), so the demo needs nothing beyond pip. That catalog is private to your machine — fine for a sample, wrong for a team.

For a shared setup, point `--catalog-uri` (or `ICEBERG_CATALOG_URI`) at a REST catalog and every engine sees the same tables:

```bash
python3 iceberg_query.py --catalog-uri http://localhost:8181 --register
```

Anything speaking the Iceberg REST protocol works: Nessie, Polaris, Lakekeeper, AWS Glue, Unity Catalog. Recent SeaweedFS builds also serve an Iceberg REST catalog on port 8181 alongside the S3 endpoint.

### Reading the same table from other engines

Once registered, the table is ordinary Iceberg. Nothing about it is Bitquery-specific:

```sql
-- Trino / Athena
SELECT Trade_Dex_ProtocolName, count(*)
FROM iceberg.ethereum.dex_trades
GROUP BY 1;
```

```python
# Spark
spark.table("ethereum.dex_trades").groupBy("Trade_Dex_ProtocolName").count().show()
```

### Iceberg options

| Flag | Default | Purpose |
| --- | --- | --- |
| `--endpoint` | `DATA_LAKE_ENDPOINT` env | S3 endpoint of the data lake |
| `--bucket` | `archive` | Bucket name |
| `--key` | `ethereum/dex_trades/24053500_24053549.parquet` | Parquet object key to register; repeatable |
| `--table` | `ethereum.dex_trades` | Iceberg table identifier |
| `--catalog-uri` | `ICEBERG_CATALOG_URI` env, else `sqlite:///iceberg_catalog.db` | Catalog URI; an `http(s)` URL uses a REST catalog |
| `--register` | off | Create the table and register the Parquet files |
| `--replace` | off | With `--register`, drop and recreate an existing table |
| `--schema` | off | Print the table schema |
| `--metadata` | off | Print snapshots and per-file statistics |
| `--examples` | off | Run the built-in example queries |
| `--sql` | none | Run one SQL statement against the table |
| `--where` | none | Iceberg row filter pushed into the scan |

With no flags at all, the script does `--register --schema --examples`.

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

These are `stream.py` flags. `iceberg_query.py` has its own set, listed above.

## Decode the local sample without streaming

The demo image bakes the block into `/seed/`. You can also decode a downloaded `.block.lz4` directly:

```python
import lz4.frame
from evm.block_message_pb2 import BlockMessage

raw = open("sample/000046600927_0x0133403c4fe53c434b1d2a1686d339eebd4e8e7f50ab52ab84cd68029e82e955_49e9339dd61bdb91320044378bff935efd925d868ca257ef8c3bc42177f9fd44.block.lz4", "rb").read()
block = BlockMessage()
block.ParseFromString(lz4.frame.decompress(raw))
print(int.from_bytes(block.Header.Number, "big"), len(block.Transactions))
```

## Links

- [Data Lake documentation](https://docs.bitquery.io/docs/data-lake/)
- [Demo image on Docker Hub](https://hub.docker.com/r/marketingbitquery/datalake-demo)
- [Block schema](https://github.com/bitquery/streaming_protobuf)
- [SeaweedFS](https://github.com/seaweedfs/seaweedfs)
- [Cloud dump schemas](https://github.com/bitquery/blockchain-cloud-data-dump-sample) — the Parquet table layouts per chain
- [Apache Iceberg](https://iceberg.apache.org/) and [PyIceberg](https://py.iceberg.apache.org/)
