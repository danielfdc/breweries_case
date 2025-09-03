-- Gold table: Aggregated data
CREATE TABLE IF NOT EXISTS nessie.gold_layer.tbl_gold_brewery_agg (
    brewery_type STRING COMMENT 'Type of brewery',
    country STRING COMMENT 'Country',
    state STRING COMMENT 'State/Province', 
    city STRING COMMENT 'City',
    brewery_count BIGINT COMMENT 'Count of breweries',
    unique_brewery_count BIGINT COMMENT 'Count of unique breweries',
    extraction_date DATE COMMENT 'Date when data was extracted'
)
USING iceberg
PARTITIONED BY (extraction_date)
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='snappy'
);