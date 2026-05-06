#!/bin/bash
# Bootstrap the OpenSearch keystore with S3 credentials before node starts.
# S3 access_key and secret_key are "secure settings" in OpenSearch — they MUST
# live in the keystore, not plain opensearch.yml or environment variables.
set -e

KEYSTORE_BIN=/usr/share/opensearch/bin/opensearch-keystore

# Create keystore if this is a fresh data volume
$KEYSTORE_BIN create --silent 2>/dev/null || true

# Inject credentials (--force = overwrite on restart, idempotent)
echo "${S3_ACCESS_KEY:-minioadmin}" | $KEYSTORE_BIN add --stdin --force s3.client.default.access_key
echo "${S3_SECRET_KEY:-minioadmin}" | $KEYSTORE_BIN add --stdin --force s3.client.default.secret_key

# Hand off to the official OpenSearch Docker entrypoint
exec /usr/share/opensearch/opensearch-docker-entrypoint.sh "$@"
