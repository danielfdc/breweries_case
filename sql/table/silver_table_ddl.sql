-- Silver table: Curated data
CREATE TABLE IF NOT EXISTS nessie.silver_layer.tbl_silver_brewery (
    id STRING COMMENT 'Brewery unique identifier',
    name STRING COMMENT 'Brewery name',
    brewery_type STRING COMMENT 'Type of brewery',
    city STRING COMMENT 'City location',
    state STRING COMMENT 'State/Province',
    country STRING COMMENT 'Country',
    postal_code STRING COMMENT 'Postal code',
    longitude DOUBLE COMMENT 'Longitude coordinate',
    latitude DOUBLE COMMENT 'Latitude coordinate',
    phone STRING COMMENT 'Phone number (digits only)',
    website_url STRING COMMENT 'Website URL',
    extraction_date DATE COMMENT 'Date when data was extracted'
)
USING iceberg
PARTITIONED BY (extraction_date)
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='snappy'
);