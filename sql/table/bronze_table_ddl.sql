-- Bronze table: Raw JSON data
CREATE TABLE IF NOT EXISTS nessie.bronze_layer.tbl_bronze_breweries (
    raw_json STRING COMMENT 'Raw JSON from API',
    extraction_date DATE COMMENT 'Date when data was extracted'
)
USING iceberg
PARTITIONED BY (extraction_date)
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='snappy'
);