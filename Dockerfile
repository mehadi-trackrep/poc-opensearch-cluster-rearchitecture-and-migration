FROM --platform=linux/amd64 docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2

# repository-s3 plugin is not bundled — install it so snapshot/restore to
# S3 (MinIO locally, real AWS S3 in production) works.
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install --batch repository-s3
