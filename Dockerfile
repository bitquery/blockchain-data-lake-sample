# Self-seeding SeaweedFS demo lake for the Bitquery Blockchain Data Lake.
#
# Boots a SeaweedFS S3 endpoint with the sample data already loaded, so:
#   docker run -p 8333:8333 marketingbitquery/datalake-demo
#
# gives a lake at http://localhost:8333 holding both object types the lake
# serves: raw blocks (Protobuf + LZ4) to stream with stream.py, and Parquet
# dumps to query as an Iceberg table with iceberg_query.py.

FROM chrislusf/seaweedfs:latest

# tools used only at boot to create the bucket and upload the block
RUN apk add --no-cache aws-cli curl

# bake the sample blocks into the image
COPY sample/*.block.lz4 /seed/

# bake the sample Parquet dumps in, keeping their chain/table/ prefix so the
# lake layout matches production: <chain>/<table>/<block-range>.parquet
COPY sample/parquet/ /seed-parquet/

# boot script that starts SeaweedFS and loads the block
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# S3 (8333), master (9333), filer (8888)
EXPOSE 8333 9333 8888

ENTRYPOINT ["/entrypoint.sh"]
