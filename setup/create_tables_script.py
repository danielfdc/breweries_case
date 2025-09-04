"""Script to create Iceberg tables in Nessie catalog."""
from __future__ import annotations

import logging
import sys
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with Nessie/Iceberg configuration."""
    return SparkSession.builder \
        .appName("CreateBreweryTables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()


def create_namespace(spark: SparkSession, nm_namespace: str) -> None:
    """Create namespace if not exists."""
    logger.info("Creating namespace nessie.<name> if not exists")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{nm_namespace};")

"""
def drop_tables_if_exist(spark: SparkSession, drop_existing: bool = False) -> None:
    TODO - Refatorar depois. 
    #Optionally drop existing tables.
    if not drop_existing:
        return
    
    tables = ["bronze", "silver", "gold"]
    for table in tables:
        logger.info(f"Dropping table nessie.breweries.{table} if exists")
        spark.sql(f"DROP TABLE IF EXISTS nessie.breweries.{table};")
"""

def create_bronze_table(spark: SparkSession) -> None:
    """Create bronze layer table."""
    logger.info("Creating bronze table")
    
    bronze_schema = StructType([
        StructField("raw_json", StringType(), False),
        StructField("extraction_date", DateType(), False)
    ])
    
    # Create empty DataFrame with schema
    df_bronze = spark.createDataFrame([], bronze_schema)
    
    # Write as Iceberg table
    df_bronze.writeTo("nessie.bronze_layer.tbl_bronze_breweries") \
        .using("iceberg") \
        .partitionedBy("extraction_date") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .createOrReplace()
    
    logger.info("Bronze table created successfully")


def create_silver_table(spark: SparkSession) -> None:
    """Create silver layer table."""
    logger.info("Creating silver table")
    
    silver_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("extraction_date", DateType(), False)
    ])
    
    df_silver = spark.createDataFrame([], silver_schema)
    
    df_silver.writeTo("nessie.silver_layer.tbl_silver_brewery") \
        .using("iceberg") \
        .partitionedBy("extraction_date") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .createOrReplace()
    
    logger.info("Silver table created successfully")


def create_gold_table(spark: SparkSession) -> None:
    """Create gold layer table."""
    logger.info("Creating gold table")
    
    gold_schema = StructType([
        StructField("brewery_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("brewery_count", LongType(), False),
        StructField("unique_brewery_count", LongType(), False),
        StructField("extraction_date", DateType(), False)
    ])
    
    df_gold = spark.createDataFrame([], gold_schema)
    
    df_gold.writeTo("nessie.gold_layer.tbl_gold_brewery_agg") \
        .using("iceberg") \
        .partitionedBy("extraction_date") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .createOrReplace()
    
    logger.info("Gold table created successfully")

"""
def verify_tables(spark: SparkSession) -> None:
    TODO - Ajustar lógica depois.
    #Verify that all tables were created.
    logger.info("Verifying tables...")
    
    tables = ["bronze", "silver", "gold"]
    for table in tables:
        try:
            count = spark.table(f"nessie.breweries.{table}").count()
            logger.info(f"✓ Table nessie.breweries.{table} exists (rows: {count})")
        except Exception as e:
            logger.error(f"✗ Table nessie.breweries.{table} verification failed: {e}")
            raise
"""

def main(drop_existing: bool = False) -> None:
    """Main execution function."""
    logger.info("Starting table creation process")
    
    bronze_namespace = "bronze_layer"
    silver_namespace = "silver_layer"
    gold_namespace = "gold_layer"
    spark = None
    try:
        spark = create_spark_session()
        create_namespace(spark, bronze_namespace)
        create_namespace(spark, silver_namespace)
        create_namespace(spark, gold_namespace)
        #drop_tables_if_exist(spark, drop_existing)
        create_bronze_table(spark)
        create_silver_table(spark)
        create_gold_table(spark)
        #verify_tables(spark)
        logger.info("All tables created successfully")
    except Exception as e:
        logger.error(f"Table creation failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    # Parse command line args
    drop_existing = "--drop" in sys.argv
    if drop_existing:
        logger.warning("Will drop existing tables before creating")
    
    main(drop_existing)
