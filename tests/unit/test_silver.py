"""Tests for silver layer processors."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processors.silver_processors import process_silver_layer


class TestProcessSilverLayer:
    """Test silver layer processing."""
    
    def setup_bronze_data(
        self, spark: SparkSession, sample_brewery_data: list[dict], test_date: date
    ) -> None:
        """Helper to setup bronze table with test data."""
        data = [(json.dumps(brewery), test_date) for brewery in sample_brewery_data]
        df = spark.createDataFrame(data, ["raw_json", "extraction_date"])
        df.write.mode("overwrite").saveAsTable("test_bronze")
    
    def test_process_silver_layer(
        self, spark: SparkSession, sample_brewery_data, test_date
    ):
        """Test silver layer data transformation."""
        self.setup_bronze_data(spark, sample_brewery_data, test_date)
        
        with patch("processors.silver_processors.BRONZE_TABLE", "test_bronze"), \
             patch("processors.silver_processors.SILVER_TABLE", "test_silver"):
            process_silver_layer(spark, test_date)
        
        df_silver = spark.table("test_silver")
        
        # Verify record count
        assert df_silver.count() == 3
        
        # Verify schema
        expected_columns = [
            "id", "name", "brewery_type", "city", "state", "country",
            "postal_code", "longitude", "latitude", "phone", "website_url",
            "extraction_date"
        ]
        assert set(df_silver.columns) == set(expected_columns)
        
        # Verify transformations
        portland_breweries = df_silver.filter(F.col("city") == "Portland").collect()
        assert len(portland_breweries) == 2
        
        # Check normalizations
        first_brewery = df_silver.filter(F.col("name") == "Brewery One").first()
        assert first_brewery["brewery_type"] == "micro"  # lowercase
        assert first_brewery["state"] == "OREGON"  # uppercase
        assert first_brewery["country"] == "UNITED STATES"  # uppercase
        assert first_brewery["phone"] == "5035550001"  # digits only
    
    def test_silver_null_handling(
        self, spark: SparkSession, sample_brewery_data, test_date
    ):
        """Test handling of null values."""
        self.setup_bronze_data(spark, sample_brewery_data, test_date)
        
        with patch("processors.silver_processors.BRONZE_TABLE", "test_bronze"), \
             patch("processors.silver_processors.SILVER_TABLE", "test_silver_nulls"):
            process_silver_layer(spark, test_date)
        
        df_silver = spark.table("test_silver_nulls")
        
        # Check that records with null coordinates still exist
        brewery3 = df_silver.filter(F.col("name") == "Brewery Three").first()
        assert brewery3 is not None
        assert brewery3["longitude"] is None
        assert brewery3["latitude"] is None
        assert brewery3["phone"] == ""  # Empty string after cleaning
    
    def test_silver_data_quality(
        self, spark: SparkSession, test_date
    ):
        """Test data quality rules."""
        # Create bronze data with quality issues
        bad_data = [
            {
                "id": "  id-with-spaces  ",
                "name": "  Brewery With Spaces  ",
                "brewery_type": "  MICRO  ",
                "city": "  Portland  ",
                "state_province": "  oregon  ",
                "country": "  united states  ",
                "phone": "(503) 555-0001",
                "postal_code": "  97201  "
            }
        ]
        
        data = [(json.dumps(brewery), test_date) for brewery in bad_data]
        df = spark.createDataFrame(data, ["raw_json", "extraction_date"])
        df.write.mode("overwrite").saveAsTable("test_bronze_quality")
        
        with patch("processors.silver_processors.BRONZE_TABLE", "test_bronze_quality"), \
             patch("processors.silver_processors.SILVER_TABLE", "test_silver_quality"):
            process_silver_layer(spark, test_date)
        
        df_silver = spark.table("test_silver_quality")
        row = df_silver.first()
        
        # Verify trimming and normalization
        assert row["id"] == "id-with-spaces"  # trimmed
        assert row["name"] == "Brewery With Spaces"  # trimmed
        assert row["brewery_type"] == "micro"  # lowercase and trimmed
        assert row["city"] == "Portland"  # trimmed
        assert row["state"] == "OREGON"  # uppercase and trimmed
        assert row["country"] == "UNITED STATES"  # uppercase and trimmed
        assert row["phone"] == "5035550001"  # digits only
        assert row["postal_code"] == "97201"  # trimmed