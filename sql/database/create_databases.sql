-- Create databases following medallion architecture
-- Execute via Trino CLI: trino --server http://localhost:8082 --catalog iceberg --schema default

-- Create Bronze database/schema
CREATE DATABASE IF NOT EXISTS nessie.bronze_layer
location 's3a://warehouse/bronze_layer.db/';

-- Create Bronze database/schema
CREATE DATABASE IF NOT EXISTS nessie.silver_layer
location 's3a://warehouse/silver_layer.db/';

-- Create Bronze database/schema
CREATE DATABASE IF NOT EXISTS nessie.gold_layer
location 's3a://warehouse/gold_layer.db/';

