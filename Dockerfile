# filepath: /home/zenilson-felipe/projetos/pipeline_antaq/Dockerfile
FROM quay.io/astronomer/astro-runtime:12.7.1

# Install the MinIO Python SDK
RUN pip install minio