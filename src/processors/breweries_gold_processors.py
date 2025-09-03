"""Gold layer processor for brewery aggregations."""
from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.breweries_pipeline_configs import GOLD_TABLE, SILVER_TABLE

logger = logging.getLogger(__name__)


def process_gold_layer(spark: SparkSession, extraction_date: date) -> None:
    """Aggregate brewery data for analytical layer."""
    logger.info(f"Starting gold layer processing for {extraction_date}")
    
    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # Read from silver layer for specific partition
    df_silver = spark.table(SILVER_TABLE).filter(
        F.col("extraction_date") == extraction_date
    )
    
    # Aggregate by brewery type and location
    df_gold_output = df_silver.groupBy(
        "brewery_type",
        "country",
        "state",
        "city",
        "extraction_date"
    ).agg(
        F.count("*").alias("brewery_count"),
        F.collect_set("id").alias("brewery_ids")  # Keep track of which breweries
    ).select(
        F.col("brewery_type"),
        F.col("country"),
        F.col("state"),
        F.col("city"),
        F.col("brewery_count"),
        F.size("brewery_ids").alias("unique_brewery_count"),
        F.col("extraction_date")
    )
    
    # Write to gold table
    df_gold_output.write \
        .mode("overwrite") \
        .partitionBy("extraction_date") \
        .format("iceberg") \
        .saveAsTable(GOLD_TABLE)
    
    row_count = df_gold_output.count()
    total_breweries = df_gold_output.agg(F.sum("brewery_count")).collect()[0][0]
    logger.info(f"Gold layer completed: {row_count} aggregated records, {total_breweries} total breweries for {extraction_date}")