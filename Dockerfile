FROM opensearchproject/opensearch:1.3.14

# repository-s3 is not bundled in the base image — install it so snapshot/restore
# to S3 (and MinIO) works. This is identical to what production nodes need.
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch repository-s3
