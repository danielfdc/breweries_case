"""End-to-end integration tests for the brewery pipeline."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processors.bronze_processors import process_bronze_layer
from processors.gold_processors import process_gold_layer
from processors.silver_processors import process_silver_layer


class TestPipelineIntegration:
    """Test complete pipeline integration."""
    
    @pytest.fixture
    def large_brewery_dataset(self) -> list[dict]:
        """Generate larger dataset for integration testing."""
        breweries = []
        brewery_types = ["micro", "nano", "regional", "brewpub", "large", "planning"]
        cities = [
            ("Portland", "Oregon", "United States"),
            ("Seattle", "Washington", "United States"),
            ("San Francisco", "California", "United States"),
            ("Austin", "Texas", "United States"),
            ("Denver", "Colorado", "United States")
        ]
        
        for i in range(100):
            city_idx = i % len(cities)
            type_idx = i % len(brewery_types)
            city, state, country = cities[city_idx]
            
            brewery = {
                "id": f"brewery-{i:04d}",
                "name": f"Test Brewery {i}",
                "brewery_type": brewery_types[type_idx],
                "address_1": f"{i} Main St",
                "city": city,
                "state_province": state,
                "postal_code": f"{90000 + i}",
                "country": country,
                "longitude": f"-122.{i:04d}",
                "latitude": f"45.{i:04d}",
                "phone": f"555-{i:04d}",
                "website_url": f"http://brewery{i}.com"
            }
            breweries.append(brewery)
        
        return breweries
    
    @patch("processors.bronze_processors.fetch_breweries_from_api")
    def test_complete_pipeline(
        self, mock_fetch, spark: SparkSession, large_brewery_dataset, test_date
    ):
        """Test complete pipeline from bronze to gold."""
        mock_fetch.return_value = large_brewery_dataset
        
        # Configure test table names
        bronze_table = "test_int_bronze"
        silver_table = "test_int_silver"
        gold_table = "test_int_gold"
        
        # Process Bronze Layer
        with patch("processors.bronze_processors.BRONZE_TABLE", bronze_table):
            process_bronze_layer(spark, test_date)
        
        # Verify Bronze
        df_bronze = spark.table(bronze_table)
        assert df_bronze.count() == 100
        assert df_bronze.filter(F.col("extraction_date") == test_date).count() == 100
        
        # Process Silver Layer
        with patch("processors.silver_processors.BRONZE_TABLE", bronze_table), \
             patch("processors.silver_processors.SILVER_TABLE", silver_table):
            process_silver_layer(spark, test_date)
        
        # Verify Silver
        df_silver = spark.table(silver_table)
        assert df_silver.count() == 100
        assert df_silver.filter(F.col("state") == "OREGON").count() == 20
        assert df_silver.filter(F.col("brewery_type") == "micro").count() > 0
        
        # Process Gold Layer
        with patch("processors.gold_processors.SILVER_TABLE", silver_table), \
             patch("processors.gold_processors.GOLD_TABLE", gold_table):
            process_gold_layer(spark, test_date)
        
        # Verify Gold
        df_gold = spark.table(gold_table)
        assert df_gold.count() > 0
        assert df_gold.count() <= 30  # Max possible combinations (6 types * 5 cities)
        
        # Verify aggregation correctness
        total_in_gold = df_gold.agg(F.sum("brewery_count")).collect()[0][0]
        assert total_in_gold == 100
        
        # Verify specific aggregation
        portland_micro = df_gold.filter(
            (F.col("city") == "Portland") & 
            (F.col("brewery_type") == "micro")
        ).first()
        assert portland_micro is not None
        assert portland_micro["brewery_count"] > 0
    
    @patch("processors.bronze_processors.fetch_breweries_from_api")
    def test_pipeline_idempotency(
        self, mock_fetch, spark: SparkSession, sample_brewery_data, test_date
    ):
        """Test pipeline idempotency - rerunning doesn't duplicate data."""
        mock_fetch.return_value = sample_brewery_data
        
        bronze_table = "test_idem_bronze"
        silver_table = "test_idem_silver"
        gold_table = "test_idem_gold"
        
        # Run pipeline twice
        for run in range(2):
            # Bronze
            with patch("processors.bronze_processors.BRONZE_TABLE", bronze_table):
                process_bronze_layer(spark, test_date)
            
            # Silver
            with patch("processors.silver_processors.BRONZE_TABLE", bronze_table), \
                 patch("processors.silver_processors.SILVER_TABLE", silver_table):
                process_silver_layer(spark, test_date)
            
            # Gold
            with patch("processors.gold_processors.SILVER_TABLE", silver_table), \
                 patch("processors.gold_processors.GOLD_TABLE", gold_table):
                process_gold_layer(spark, test_date)
        
        # Verify no duplication
        assert spark.table(bronze_table).count() == 3
        assert spark.table(silver_table).count() == 3
        assert spark.table(gold_table).filter(
            F.col("extraction_date") == test_date
        ).count() <= 3  # At most 3 aggregation groups
    
    @patch("processors.bronze_processors.fetch_breweries_from_api")
    def test_multiple_dates_partitioning(
        self, mock_fetch, spark: SparkSession, sample_brewery_data
    ):
        """Test handling multiple extraction dates."""
        mock_fetch.return_value = sample_brewery_data
        
        bronze_table = "test_part_bronze"
        silver_table = "test_part_silver"
        gold_table = "test_part_gold"
        
        dates = [
            date(2024, 1, 15),
            date(2024, 1, 16),
            date(2024, 1, 17)
        ]
        
        # Process each date
        for process_date in dates:
            # Bronze
            with patch("processors.bronze_processors.BRONZE_TABLE", bronze_table):
                process_bronze_layer(spark, process_date)
            
            # Silver
            with patch("processors.silver_processors.BRONZE_TABLE", bronze_table), \
                 patch("processors.silver_processors.SILVER_TABLE", silver_table):
                process_silver_layer(spark, process_date)
            
            # Gold
            with patch("processors.gold_processors.SILVER_TABLE", silver_table), \
                 patch("processors.gold_processors.GOLD_TABLE", gold_table):
                process_gold_layer(spark, process_date)
        
        # Verify partitioning
        df_bronze = spark.table(bronze_table)
        df_silver = spark.table(silver_table)
        df_gold = spark.table(gold_table)
        
        # Each date should have its partition
        for process_date in dates:
            assert df_bronze.filter(F.col("extraction_date") == process_date).count() == 3
            assert df_silver.filter(F.col("extraction_date") == process_date).count() == 3
            assert df_gold.filter(F.col("extraction_date") == process_date).count() > 0
        
        # Total should be 3 dates * 3 records
        assert df_bronze.count() == 9
        assert df_silver.count() == 9