"""Tests for gold layer processors."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from processors.gold_processors import process_gold_layer


class TestProcessGoldLayer:
    """Test gold layer processing."""
    
    def setup_silver_data(self, spark: SparkSession, test_date: date) -> None:
        """Helper to setup silver table with test data."""
        schema = StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("brewery_type", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("country", StringType()),
            StructField("postal_code", StringType()),
            StructField("longitude", DoubleType()),
            StructField("latitude", DoubleType()),
            StructField("phone", StringType()),
            StructField("website_url", StringType()),
            StructField("extraction_date", DateType())
        ])
        
        data = [
            ("1", "Brewery A", "micro", "Portland", "OR", "US", "97201", 
             -122.67, 45.51, "5035551111", "http://a.com", test_date),
            ("2", "Brewery B", "micro", "Portland", "OR", "US", "97202", 
             -122.68, 45.50, "5035552222", "http://b.com", test_date),
            ("3", "Brewery C", "brewpub", "Seattle", "WA", "US", "98101", 
             -122.33, 47.61, "2065553333", "http://c.com", test_date),
            ("4", "Brewery D", "brewpub", "Seattle", "WA", "US", "98102", 
             -122.31, 47.62, "2065554444", "http://d.com", test_date),
            ("5", "Brewery E", "regional", "Seattle", "WA", "US", "98103", 
             -122.34, 47.63, "2065555555", "http://e.com", test_date),
        ]
        
        df = spark.createDataFrame(data, schema)
        df.write.mode("overwrite").saveAsTable("test_silver")
    
    def test_process_gold_layer(self, spark: SparkSession, test_date: date):
        """Test gold layer aggregation."""
        self.setup_silver_data(spark, test_date)
        
        with patch("processors.gold_processors.SILVER_TABLE", "test_silver"), \
             patch("processors.gold_processors.GOLD_TABLE", "test_gold"):
            process_gold_layer(spark, test_date)
        
        df_gold = spark.table("test_gold")
        
        # Verify schema
        expected_columns = [
            "brewery_type", "country", "state", "city", 
            "brewery_count", "unique_brewery_count", "extraction_date"
        ]
        assert set(df_gold.columns) == set(expected_columns)
        
        # Verify aggregations
        assert df_gold.count() == 4  # 4 unique combinations
        
        # Check Portland micro breweries
        portland_micro = df_gold.filter(
            (F.col("city") == "Portland") & 
            (F.col("brewery_type") == "micro")
        ).first()
        assert portland_micro["brewery_count"] == 2
        assert portland_micro["unique_brewery_count"] == 2
        
        # Check Seattle brewpubs
        seattle_brewpub = df_gold.filter(
            (F.col("city") == "Seattle") & 
            (F.col("brewery_type") == "brewpub")
        ).first()
        assert seattle_brewpub["brewery_count"] == 2
        assert seattle_brewpub["unique_brewery_count"] == 2
        
        # Check Seattle regional
        seattle_regional = df_gold.filter(
            (F.col("city") == "Seattle") & 
            (F.col("brewery_type") == "regional")
        ).first()
        assert seattle_regional["brewery_count"] == 1
        assert seattle_regional["unique_brewery_count"] == 1
    
    def test_gold_total_counts(self, spark: SparkSession, test_date: date):
        """Test total brewery counts."""
        self.setup_silver_data(spark, test_date)
        
        with patch("processors.gold_processors.SILVER_TABLE", "test_silver"), \
             patch("processors.gold_processors.GOLD_TABLE", "test_gold_totals"):
            process_gold_layer(spark, test_date)
        
        df_gold = spark.table("test_gold_totals")
        
        # Verify total count
        total_breweries = df_gold.agg(F.sum("brewery_count")).collect()[0][0]
        assert total_breweries == 5
        
        # Verify by state
        wa_breweries = df_gold.filter(F.col("state") == "WA") \
            .agg(F.sum("brewery_count")).collect()[0][0]
        assert wa_breweries == 3
        
        or_breweries = df_gold.filter(F.col("state") == "OR") \
            .agg(F.sum("brewery_count")).collect()[0][0]
        assert or_breweries == 2
    
    def test_gold_empty_partition(self, spark: SparkSession, test_date: date):
        """Test handling of empty partition."""
        # Create empty silver table
        schema = StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("brewery_type", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("country", StringType()),
            StructField("postal_code", StringType()),
            StructField("longitude", DoubleType()),
            StructField("latitude", DoubleType()),
            StructField("phone", StringType()),
            StructField("website_url", StringType()),
            StructField("extraction_date", DateType())
        ])
        
        df_empty = spark.createDataFrame([], schema)
        df_empty.write.mode("overwrite").saveAsTable("test_silver_empty")
        
        with patch("processors.gold_processors.SILVER_TABLE", "test_silver_empty"), \
             patch("processors.gold_processors.GOLD_TABLE", "test_gold_empty"):
            process_gold_layer(spark, test_date)
        
        df_gold = spark.table("test_gold_empty")
        assert df_gold.count() == 0