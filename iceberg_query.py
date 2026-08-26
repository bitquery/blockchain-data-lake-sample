#!/usr/bin/env python3
"""
Query the Bitquery Blockchain Data Lake with an Iceberg engine.

The lake stores two kinds of objects:

  *.block.lz4   raw blocks (Protobuf + LZ4)  -> stream.py
  *.parquet     flattened dump tables        -> this script

The Parquet dumps are the ones an Iceberg engine can read. Iceberg is a table
format, not a file format: it needs columnar data files plus table metadata
(schema, snapshots, manifests) in a catalog. The Parquet files already in the
lake supply the data; this script supplies the metadata by *registering* them
into an Iceberg table with `add_files`.

Nothing is rewritten or copied. `add_files` records the existing S3 objects as
Iceberg data files, so the same bytes stream.py reads are now a queryable
table. Only the small metadata tree is written, under s3://<bucket>/iceberg/.

Once registered, any Iceberg engine can read it -- Spark, Trino, DuckDB,
Athena, Dremio, Snowflake. This script uses PyIceberg to register and DuckDB
to run SQL, because both fit in one pip install.

Usage:
    pip install -r requirements.txt

    # run the demo lake first:  docker run -p 8333:8333 marketingbitquery/datalake-demo
    export DATA_LAKE_ENDPOINT=http://localhost:8333
    export DATA_LAKE_ACCESS_KEY=admin
    export DATA_LAKE_SECRET_KEY=secret

    # register the bundled Ethereum dex_trades dump and show the table
    python iceberg_query.py --register

    # run the built-in example queries
    python iceberg_query.py --examples

    # run your own SQL against the table (the DuckDB view is named after the table)
    python iceberg_query.py --sql "
        SELECT Trade_Dex_ProtocolName, count(*) AS trades
        FROM dex_trades GROUP BY 1 ORDER BY trades DESC"

    # show what Iceberg wrote: snapshots, manifests, file-level stats
    python iceberg_query.py --metadata

By default the catalog is a local SQLite file (iceberg_catalog.db), which keeps
the demo self-contained. Point --catalog-uri at an http(s) URL to use a REST
catalog instead (Nessie, Polaris, Lakekeeper, AWS Glue's REST endpoint, or the
Iceberg REST catalog that recent SeaweedFS builds serve on port 8181); the
table metadata then lives in the shared catalog and every engine sees it.
"""

import argparse
import os
import sys

DEFAULT_KEY = "ethereum/dex_trades/24053500_24053549.parquet"
DEFAULT_TABLE = "ethereum.dex_trades"

# A guard used by the USD examples. Dump rows carry the price the DEX quoted,
# and an illiquid or spoofed pair can quote nonsense -- this 50-block sample has
# an XOR trade priced at 4.3e23 USD. Real analytics needs a sanity bound; the
# "Outlier USD amounts" example below shows what is being excluded rather than
# quietly hiding it.
SANE_USD = "Trade_Buy_AmountInUSD < 1e9"

EXAMPLES = [
    (
        "Trades per DEX protocol",
        f"""
        SELECT Trade_Dex_ProtocolName AS protocol,
               count(*)               AS trades,
               round(sum(Trade_Buy_AmountInUSD), 2) AS volume_usd
        FROM dex_trades
        WHERE {SANE_USD}
        GROUP BY 1
        ORDER BY volume_usd DESC
        LIMIT 10
        """,
    ),
    (
        "Most traded tokens by USD volume",
        f"""
        SELECT Trade_Buy_Currency_Symbol AS symbol,
               count(*)                  AS trades,
               round(sum(Trade_Buy_AmountInUSD), 2) AS volume_usd
        FROM dex_trades
        WHERE Trade_Buy_Currency_Symbol <> '' AND {SANE_USD}
        GROUP BY 1
        ORDER BY volume_usd DESC
        LIMIT 10
        """,
    ),
    (
        "Largest single trades",
        f"""
        SELECT Block_Number,
               Trade_Sell_Currency_Symbol AS sold,
               Trade_Buy_Currency_Symbol  AS bought,
               round(Trade_Buy_AmountInUSD, 2) AS usd,
               Trade_Dex_ProtocolName     AS protocol
        FROM dex_trades
        WHERE {SANE_USD}
        ORDER BY Trade_Buy_AmountInUSD DESC
        LIMIT 10
        """,
    ),
    (
        "Outlier USD amounts (excluded above)",
        f"""
        SELECT Block_Number,
               Trade_Sell_Currency_Symbol AS sold,
               Trade_Buy_Currency_Symbol  AS bought,
               Trade_Buy_AmountInUSD      AS usd,
               Trade_Dex_ProtocolName     AS protocol
        FROM dex_trades
        WHERE NOT ({SANE_USD})
        ORDER BY usd DESC
        """,
    ),
    (
        "Block range covered by this dump",
        """
        SELECT min(Block_Number) AS first_block,
               max(Block_Number) AS last_block,
               count(*)          AS trades,
               count(DISTINCT Transaction_Hash) AS transactions
        FROM dex_trades
        """,
    ),
]


def s3_properties(args):
    """FileIO settings so PyIceberg reads and writes against the lake's S3 endpoint."""
    return {
        "s3.endpoint": args.endpoint,
        "s3.access-key-id": os.environ.get("DATA_LAKE_ACCESS_KEY", ""),
        "s3.secret-access-key": os.environ.get("DATA_LAKE_SECRET_KEY", ""),
        "s3.region": args.region,
    }


def load_catalog(args):
    """Open the Iceberg catalog: SQLite by default, REST when --catalog-uri is a URL."""
    props = {"warehouse": f"s3://{args.bucket}/iceberg", **s3_properties(args)}

    if args.catalog_uri.startswith(("http://", "https://")):
        from pyiceberg.catalog.rest import RestCatalog

        return RestCatalog("lake", uri=args.catalog_uri, **props)

    from pyiceberg.catalog.sql import SqlCatalog

    return SqlCatalog("lake", uri=args.catalog_uri, **props)


def register(catalog, args):
    """Create the Iceberg table and register the Parquet objects already in the lake.

    The schema is read from the Parquet footer, so the table always matches the
    dump. add_files() then commits those objects as data files -- a metadata-only
    operation, no data movement.
    """
    import pyarrow.parquet as pq
    from pyiceberg.io.pyarrow import PyArrowFileIO

    namespace = args.table.split(".")[0]
    uris = [f"s3://{args.bucket}/{key}" for key in args.key]

    io = PyArrowFileIO(properties=s3_properties(args))
    with io.new_input(uris[0]).open() as f:
        schema = pq.ParquetFile(f).schema_arrow

    catalog.create_namespace_if_not_exists(namespace)

    if catalog.table_exists(args.table):
        if not args.replace:
            print(f"  table {args.table} already exists (use --replace to recreate)")
            return catalog.load_table(args.table)
        catalog.drop_table(args.table)

    table = catalog.create_table(args.table, schema=schema)
    table.add_files(uris)

    print(f"  registered {len(uris)} Parquet file(s) as Iceberg table {args.table}")
    for uri in uris:
        print(f"    {uri}")
    print(f"  table metadata : s3://{args.bucket}/iceberg/")
    print(f"  catalog        : {args.catalog_uri}")
    return table


def show_schema(table):
    print("\n  schema:")
    for field in table.schema().fields:
        print(f"    {field.name:<34} {field.field_type}")


def show_metadata(table):
    """Print what Iceberg added on top of the raw Parquet: snapshots and file stats."""
    print("\n  snapshots:")
    for snap in table.snapshots():
        summary = snap.summary
        print(
            f"    id {snap.snapshot_id}  {summary.operation.value if summary else '?'}  "
            f"files={summary.get('added-data-files', '?') if summary else '?'} "
            f"rows={summary.get('added-records', '?') if summary else '?'}"
        )

    print("\n  data files:")
    for task in table.scan().plan_files():
        f = task.file
        print(
            f"    {f.file_path}\n"
            f"      records {f.record_count:,}  size {f.file_size_in_bytes / 1024:.0f} KB  "
            f"format {f.file_format}"
        )

    print(
        "\n  Those per-file record counts and column bounds are what let an engine\n"
        "  skip files entirely on a WHERE clause -- the thing plain Parquet in a\n"
        "  bucket cannot do, because nothing knows what is in which object."
    )


def to_duckdb(table, args, view):
    """Load the Iceberg scan into DuckDB under `view` (the table's short name).

    PyIceberg plans the scan (which files, which row groups) and hands DuckDB an
    Arrow table. Pushing --where down to the scan means unmatched files are never
    read from S3 at all.
    """
    import duckdb

    scan = table.scan(row_filter=args.where) if args.where else table.scan()
    arrow = scan.to_arrow()

    con = duckdb.connect()
    con.register(view, arrow)
    return con, arrow


def run_sql(con, sql, title=None):
    if title:
        print(f"\n  {title}")
        print(f"  {'-' * len(title)}")
    print(con.sql(sql))


def main():
    p = argparse.ArgumentParser(
        description="Register and query the lake's Parquet dumps as an Iceberg table."
    )
    p.add_argument("--endpoint", default=os.environ.get("DATA_LAKE_ENDPOINT"),
                   help="S3 endpoint of the data lake (or set DATA_LAKE_ENDPOINT).")
    p.add_argument("--bucket", default="archive", help="Bucket name (default: archive).")
    p.add_argument("--key", action="append", default=None, metavar="KEY",
                   help=f"Parquet object key to register; repeatable "
                        f"(default: {DEFAULT_KEY}).")
    p.add_argument("--region", default=os.environ.get("DATA_LAKE_REGION", "us-east-1"))
    p.add_argument("--table", default=DEFAULT_TABLE,
                   help=f"Iceberg table identifier (default: {DEFAULT_TABLE}).")
    p.add_argument("--catalog-uri", default=os.environ.get("ICEBERG_CATALOG_URI",
                                                           "sqlite:///iceberg_catalog.db"),
                   help="Catalog URI. SQLite by default; an http(s) URL uses a REST catalog.")
    p.add_argument("--register", action="store_true",
                   help="Create the table and register the Parquet files (idempotent).")
    p.add_argument("--replace", action="store_true",
                   help="With --register, drop and recreate the table if it exists.")
    p.add_argument("--schema", action="store_true", help="Print the table schema.")
    p.add_argument("--metadata", action="store_true",
                   help="Print snapshots and per-file statistics.")
    p.add_argument("--examples", action="store_true", help="Run the built-in example queries.")
    p.add_argument("--sql", help="Run one SQL statement; the DuckDB view is named after the "
                   "table, e.g. dex_trades or transfers.")
    p.add_argument("--where", metavar="EXPR",
                   help="Iceberg row filter pushed into the scan, e.g. "
                        "\"Trade_Buy_Currency_Symbol == 'WETH'\".")
    args = p.parse_args()

    if not args.endpoint:
        p.error("no endpoint: pass --endpoint or set DATA_LAKE_ENDPOINT")
    if args.key is None:
        args.key = [DEFAULT_KEY]

    # nothing asked for: do the whole tour
    if not any([args.register, args.schema, args.metadata, args.examples, args.sql]):
        args.register = args.schema = args.examples = True

    catalog = load_catalog(args)

    if args.register:
        table = register(catalog, args)
    else:
        if not catalog.table_exists(args.table):
            sys.exit(f"table {args.table} not found; run with --register first")
        table = catalog.load_table(args.table)

    if args.schema:
        show_schema(table)

    if args.metadata:
        show_metadata(table)

    if args.examples or args.sql:
        view = args.table.split(".")[-1]
        con, arrow = to_duckdb(table, args, view)
        print(f"\n  scanned {arrow.num_rows:,} rows into DuckDB as `{view}`"
              + (f" (row filter: {args.where})" if args.where else ""))

        if args.sql:
            run_sql(con, args.sql)
        if args.examples:
            if view == "dex_trades":
                for title, sql in EXAMPLES:
                    run_sql(con, sql, title)
            else:
                print(f"\n  --examples are written for the dex_trades table; this is "
                      f"`{view}`.\n  Query it with --sql, e.g.:\n"
                      f"    --sql \"SELECT * FROM {view} LIMIT 5\"")


if __name__ == "__main__":
    main()
