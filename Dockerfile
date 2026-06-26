# Self-seeding SeaweedFS demo lake for the Bitquery Blockchain Data Lake.
#
# Boots a SeaweedFS S3 endpoint with the sample block already loaded, so:
#   docker run -p 8333:8333 marketingbitquery/datalake-demo
# gives a lake at http://localhost:8333 with the block ready to stream.

FROM chrislusf/seaweedfs:latest

# tools used only at boot to create the bucket and upload the block
RUN apk add --no-cache aws-cli curl

# bake the sample block into the image
COPY *.block.lz4 /seed/

# boot script that starts SeaweedFS and loads the block
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# S3 (8333), master (9333), filer (8888)
EXPOSE 8333 9333 8888

ENTRYPOINT ["/entrypoint.sh"]
