#!/bin/sh
set -e

# Wait for MinIO to be ready
until mc alias set myminio http://minio:9000 admin admin123; do
  echo "Waiting for MinIO..."
  sleep 5
done

# Create buckets
mc mb myminio/spark-data --ignore-existing
mc mb myminio/warehouse --ignore-existing

# Set bucket policies
mc anonymous set public myminio/spark-data
mc anonymous set public myminio/warehouse

echo "MinIO setup completed!"
