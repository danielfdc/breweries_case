"""Performance and data quality tests."""
from __future__ import annotations

import json
import time
from datetime import date
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processors.bronze_processors import process_bronze_layer
from processors.gold_processors import process_gold_layer
from processors.silver_processors import process_silver_layer


class TestPerformance:
    """Test pipeline performance characteristics."""
    
    @pytest.fixture
    def massive_dataset(self) -> list[dict]:
        """Generate massive dataset for performance testing."""
        return [
            {
                "id": f"perf-{i:06d}",
                "name": f"Perf Brewery {i}",
                "brewery_type": ["micro", "nano", "regional"][i % 3],
                "city": f"City{i % 100}",
                "state_province": f"State{i % 50}",
                "country": "US",
                "postal_code": f"{10000 + i}",
                "longitude": f"-122.{i % 1000:04d}",
                "latitude": f"45.{i % 1000:04d}"
            }
            for i in range(10000)
        ]
    
    @patch("processors.bronze_processors.fetch_breweries_from_api")
    def test_large_dataset_performance(
        self, mock_fetch, spark: SparkSession, massive_dataset, test_date
    ):
        """Test pipeline performance with large dataset."""
        mock_fetch.return_value = massive_dataset
        
        bronze_table = "test_perf_bronze"
        silver_table = "test_perf_silver"
        gold_table = "test_perf_gold"
        
        # Measure Bronze processing time
        start = time.time()
        with patch("processors.bronze_processors.BRONZE_TABLE", bronze_table):
            process_bronze_layer(spark, test_date)
        bronze_time = time.time() - start
        
        # Measure Silver processing time
        start = time.time()
        with patch("processors.silver_processors.BRONZE_TABLE", bronze_table), \
             patch("processors.silver_processors.SILVER_TABLE", silver_table):
            process_silver_layer(spark, test_date)
        silver_time = time.time() - start
        
        # Measure Gold processing time
        start = time.time()
        with patch("processors.gold_processors.SILVER_TABLE", silver_table), \
             patch("processors.gold_processors.GOLD_TABLE", gold_table):
            process_gold_layer(spark, test_date)
        gold_time = time.time() - start
        
        # Performance assertions (adjust thresholds based on environment)
        assert bronze_time < 30, f"Bronze took {bronze_time:.2f}s, expected < 30s"
        assert silver_time < 30, f"Silver took {silver_time:.2f}s, expected < 30s"
        assert gold_time < 30, f"Gold took {gold_time:.2f}s, expected < 30s"
        
        # Verify correctness
        assert spark.table(bronze_table).count() == 10000
        assert spark.table(silver_table).count() == 10000
        df_gold = spark.table(gold_table)
        total_breweries = df_gold.agg(F.sum("brewery_count")).collect()[0][0]
        assert total_breweries == 10000


class TestDataQuality:
    """Test data quality validations."""
    
    @patch("processors.bronze_processors.fetch_breweries_from_api")
    def test_invalid_json_handling(
        self, mock_fetch, spark: SparkSession, test_date
    ):
        """Test handling of malformed data."""
        # Mix valid and invalid data
        mixed_data = [
            {"id": "valid-1", "name": "Valid Brewery"},
            {"id": None, "name": "Invalid ID"},  # null ID
            {"id": "", "name": "Empty ID"},  # empty ID
            {"id": "valid-2", "name": "Another Valid"}
        ]
        mock_fetch.return_value = mixed_data
        
        bronze_table = "test_quality_bronze"
        silver_table = "test_quality_silver"
        
        # Process Bronze (should accept all)
        with patch("processors.bronze_processors.BRONZE_TABLE", bronze_table):
            process_bronze_layer(spark, test_date)
        assert spark.table(bronze_table).count() == 4
        
        # Process Silver (should filter invalid)
        with patch("processors.silver_processors.BRONZE_TABLE", bronze_table), \
             patch("processors.silver_processors.SILVER_TABLE", silver_table):
            process_silver_layer(spark, test_date)
        
        df_silver = spark.table(silver_table)
        assert df_silver.count() == 2  # Only valid records
        assert df_silver.filter(F.col("id").isNull()).count() == 0
        assert df_silver.filter(F.col("id") == "").count() == 0
    
    def test_duplicate_handling(
        self, spark: SparkSession, test_date
    ):
        """Test handling of duplicate brewery IDs."""
        # Create silver data with duplicates
        data = [
            ("dup-1", "Brewery A", "micro", "Portland", "OR", "US", test_date),
            ("dup-1", "Brewery A Duplicate", "micro", "Portland", "OR", "US", test_date),
            ("dup-2", "Brewery B", "nano", "Seattle", "WA", "US", test_date),
        ]
        
        df = spark.createDataFrame(
            data, 
            ["id", "name", "brewery_type", "city", "state", "country", "extraction_date"]
        )
        df.write.mode("overwrite").saveAsTable("test_dup_silver")
        
        # Process Gold
        with patch("processors.gold_processors.SILVER_TABLE", "test_dup_silver"), \
             patch("processors.gold_processors.GOLD_TABLE", "test_dup_gold"):
            process_gold_layer(spark, test_date)
        
        df_gold = spark.table("test_dup_gold")
        
        # Check that unique count handles duplicates correctly
        portland_micro = df_gold.filter(
            (F.col("city") == "Portland") & 
            (F.col("brewery_type") == "micro")
        ).first()
        
        assert portland_micro["brewery_count"] == 2  # Total records
        assert portland_micro["unique_brewery_count"] == 1  # Unique IDs