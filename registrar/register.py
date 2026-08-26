#!/usr/bin/env python3
"""
One-shot registrar for the Iceberg lakehouse demo (docker-compose.yml).

Registers the sample Parquet dumps already sitting in the lake as Iceberg
tables in the REST catalog, then exits. This runs PyIceberg *inside the demo
stack* so that consumers never have to: once it finishes, any Iceberg engine
(Trino, Spark, Flink, DuckDB) reads the tables by connecting to the catalog.

`add_files` registers the existing Parquet objects as Iceberg data files -- no
rewrite, no copy. Only the small metadata tree is written, under
s3://archive/iceberg/.

Configured entirely from environment variables (see docker-compose.yml):
  CATALOG_URI, S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
"""
import os
import time

import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO

CATALOG_URI = os.environ["CATALOG_URI"]
S3 = {
    "s3.endpoint": os.environ["S3_ENDPOINT"],
    "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
    "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
    "s3.path-style-access": "true",
}
BUCKET = "archive"

# Iceberg identifier -> Parquet object key in the lake. Add rows here to
# register more dumps.
TABLES = {
    "ethereum.dex_trades": "ethereum/dex_trades/24053500_24053549.parquet",
    "ethereum.transfers": "ethereum/transfers/24053500_24053549.parquet",
}


def wait_for_catalog():
    """The catalog and lake come up alongside this job; retry until reachable."""
    last = None
    for _ in range(60):
        try:
            return RestCatalog("demo", uri=CATALOG_URI,
                               warehouse=f"s3://{BUCKET}/iceberg", **S3)
        except Exception as e:  # noqa: BLE001 - startup race, keep retrying
            last = e
            time.sleep(2)
    raise SystemExit(f"catalog not reachable at {CATALOG_URI}: {last}")


def wait_for_schema(key):
    """The lake seeds its Parquet on boot; retry until the object is readable."""
    io = PyArrowFileIO(properties=S3)
    uri = f"s3://{BUCKET}/{key}"
    for _ in range(60):
        try:
            with io.new_input(uri).open() as f:
                return pq.ParquetFile(f).schema_arrow
        except Exception:  # noqa: BLE001 - seed race, keep retrying
            time.sleep(2)
    raise SystemExit(f"object not found in lake: {uri}")


def main():
    cat = wait_for_catalog()
    for ident, key in TABLES.items():
        namespace = ident.split(".")[0]
        uri = f"s3://{BUCKET}/{key}"
        schema = wait_for_schema(key)
        cat.create_namespace_if_not_exists(namespace)
        if cat.table_exists(ident):
            print(f"[registrar] {ident} already registered, skipping")
            continue
        table = cat.create_table(ident, schema=schema)
        table.add_files([uri])
        rows = len(table.scan().to_arrow())
        print(f"[registrar] registered {ident} <- {uri}  ({rows:,} rows)")

    print("[registrar] namespaces:", cat.list_namespaces())
    print("[registrar] tables:", cat.list_tables("ethereum"))
    print(f"[registrar] done. Point your engine at the REST catalog: {CATALOG_URI}")


if __name__ == "__main__":
    main()
