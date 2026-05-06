#!/bin/bash
# Inject S3 credentials into the Elasticsearch keystore before the node starts.
# Credentials are "secure settings" — they cannot go in elasticsearch.yml or env vars.
set -e

KEYSTORE_BIN=/usr/share/elasticsearch/bin/elasticsearch-keystore

$KEYSTORE_BIN create --silent 2>/dev/null || true

echo "${S3_ACCESS_KEY:-minioadmin}" | $KEYSTORE_BIN add --stdin --force s3.client.default.access_key
echo "${S3_SECRET_KEY:-minioadmin}" | $KEYSTORE_BIN add --stdin --force s3.client.default.secret_key

exec /usr/local/bin/docker-entrypoint.sh "$@"
