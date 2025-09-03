"""Bronze layer processor for brewery data."""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType

from config.breweries_pipeline_configs import (
    BREWERY_API_BASE_URL,
    BREWERY_API_MAX_PAGES,
    BREWERY_API_PAGE_SIZE,
    BRONZE_TABLE,
)

logger = logging.getLogger(__name__)


def fetch_breweries_from_api() -> list[dict[str, Any]]:
    """Fetch all breweries from Open Brewery DB API."""
    all_breweries = []
    page = 1
    
    while page <= BREWERY_API_MAX_PAGES:
        try:
            url = f"{BREWERY_API_BASE_URL}?page={page}&per_page={BREWERY_API_PAGE_SIZE}"
            logger.info(f"Fetching page {page} from API")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            breweries = response.json()
            if not breweries:  # Empty response means no more data
                break
                
            all_breweries.extend(breweries)
            logger.info(f"Fetched {len(breweries)} breweries from page {page}")
            
            if len(breweries) < BREWERY_API_PAGE_SIZE:  # Last page
                break
                
            page += 1
            
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            raise
            
    logger.info(f"Total breweries fetched: {len(all_breweries)}")
    return all_breweries


def process_bronze_layer(spark: SparkSession, extraction_date: date) -> None:
    """Process and persist raw brewery data to bronze layer."""
    logger.info(f"Starting bronze layer processing for {extraction_date}")
    
    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # Fetch data from API
    breweries = fetch_breweries_from_api()
    
    # Define schema for bronze layer
    schema = StructType([
        StructField("raw_json", StringType(), False),
        StructField("extraction_date", DateType(), False)
    ])
    
    # Convert to DataFrame with raw JSON
    data = [(json.dumps(brewery), extraction_date) for brewery in breweries]
    df_bronze_output = spark.createDataFrame(data, schema)
    
    # Write to bronze table with partition overwrite
    df_bronze_output.write \
        .mode("overwrite") \
        .partitionBy("extraction_date") \
        .format("iceberg") \
        .saveAsTable(BRONZE_TABLE)
    
    row_count = df_bronze_output.count()
    logger.info(f"Bronze layer completed: {row_count} records written for {extraction_date}")