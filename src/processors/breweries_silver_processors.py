"""Silver layer processor for brewery data."""
from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from config.breweries_pipeline_configs import BRONZE_TABLE, SILVER_TABLE

logger = logging.getLogger(__name__)


def process_silver_layer(spark: SparkSession, extraction_date: date) -> None:
    """Transform and persist curated brewery data to silver layer."""
    logger.info(f"Starting silver layer processing for {extraction_date}")
    
    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # Read from bronze layer for specific partition
    df_bronze = spark.table(BRONZE_TABLE).filter(
        F.col("extraction_date") == extraction_date
    )
    
    # Parse JSON and extract fields
    df_parsed_tmp = df_bronze.select(
        F.get_json_object("raw_json", "$.id").alias("id"),
        F.get_json_object("raw_json", "$.name").alias("name"),
        F.get_json_object("raw_json", "$.brewery_type").alias("brewery_type"),
        F.get_json_object("raw_json", "$.address_1").alias("address"),
        F.get_json_object("raw_json", "$.city").alias("city"),
        F.get_json_object("raw_json", "$.state_province").alias("state"),
        F.get_json_object("raw_json", "$.postal_code").alias("postal_code"),
        F.get_json_object("raw_json", "$.country").alias("country"),
        F.get_json_object("raw_json", "$.longitude").cast(DoubleType()).alias("longitude"),
        F.get_json_object("raw_json", "$.latitude").cast(DoubleType()).alias("latitude"),
        F.get_json_object("raw_json", "$.phone").alias("phone"),
        F.get_json_object("raw_json", "$.website_url").alias("website_url"),
        F.col("extraction_date")
    )
    
    # Clean and normalize data
    df_silver_output = df_parsed_tmp.select(
        F.trim(F.col("id")).alias("id"),
        F.trim(F.col("name")).alias("name"),
        F.lower(F.trim(F.col("brewery_type"))).alias("brewery_type"),
        F.trim(F.col("city")).alias("city"),
        F.upper(F.trim(F.col("state"))).alias("state"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        F.trim(F.col("postal_code")).alias("postal_code"),
        F.col("longitude"),
        F.col("latitude"),
        F.regexp_replace(F.col("phone"), r"[^\d]", "").alias("phone"),  # Keep only digits
        F.trim(F.col("website_url")).alias("website_url"),
        F.col("extraction_date")
    ).filter(
        F.col("id").isNotNull()  # Ensure we have valid records
    )
    
    # Write to silver table
    df_silver_output.write \
        .mode("overwrite") \
        .partitionBy("extraction_date") \
        .format("iceberg") \
        .saveAsTable(SILVER_TABLE)
    
    row_count = df_silver_output.count()
    logger.info(f"Silver layer completed: {row_count} records written for {extraction_date}")