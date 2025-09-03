"""Tests for bronze layer processors."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processors.bronze_processors import fetch_breweries_from_api, process_bronze_layer


class TestFetchBreweriesFromAPI:
    """Test API fetching functionality."""
    
    @patch("processors.bronze_processors.requests.get")
    def test_fetch_single_page(self, mock_get, sample_brewery_data):
        """Test fetching single page of data."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_brewery_data
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        
        result = fetch_breweries_from_api()
        
        assert len(result) == 3
        assert result[0]["name"] == "Brewery One"
        mock_get.assert_called()
    
    @patch("processors.bronze_processors.requests.get")
    def test_fetch_multiple_pages(self, mock_get, sample_brewery_data):
        """Test pagination handling."""
        # First page returns full data, second returns empty
        mock_response_page1 = MagicMock()
        mock_response_page1.json.return_value = sample_brewery_data * 67  # 201 items
        mock_response_page1.raise_for_status = MagicMock()
        
        mock_response_page2 = MagicMock()
        mock_response_page2.json.return_value = sample_brewery_data[:1]  # 1 item
        mock_response_page2.raise_for_status = MagicMock()
        
        mock_get.side_effect = [mock_response_page1, mock_response_page2]
        
        result = fetch_breweries_from_api()
        
        assert len(result) == 202  # 201 + 1
        assert mock_get.call_count == 2
    
    @patch("processors.bronze_processors.requests.get")
    def test_fetch_handles_api_error(self, mock_get):
        """Test API error handling."""
        mock_get.side_effect = Exception("API Error")
        
        with pytest.raises(Exception, match="API Error"):
            fetch_breweries_from_api()


class TestProcessBronzeLayer:
    """Test bronze layer processing."""
    
    @patch("processors.bronze_processors.fetch_breweries_from_api")
    def test_process_bronze_layer(
        self, mock_fetch, spark: SparkSession, sample_brewery_data, test_date
    ):
        """Test bronze layer data processing."""
        mock_fetch.return_value = sample_brewery_data
        
        # Create temporary table name for testing
        test_table = "test_bronze"
        with patch("processors.bronze_processors.BRONZE_TABLE", test_table):
            process_bronze_layer(spark, test_date)
        
        # Verify data was written
        df = spark.table(test_table)
        assert df.count() == 3
        
        # Verify schema
        assert "raw_json" in df.columns
        assert "extraction_date" in df.columns
        
        # Verify content
        first_row = df.first()
        raw_json = json.loads(first_row["raw_json"])
        assert raw_json["name"] == "Brewery One"
        assert first_row["extraction_date"] == test_date
    
    @patch("processors.bronze_processors.fetch_breweries_from_api")
    def test_bronze_idempotency(
        self, mock_fetch, spark: SparkSession, sample_brewery_data, test_date
    ):
        """Test idempotent writes - same date overwrites."""
        mock_fetch.return_value = sample_brewery_data
        
        test_table = "test_bronze_idempotent"
        with patch("processors.bronze_processors.BRONZE_TABLE", test_table):
            # First write
            process_bronze_layer(spark, test_date)
            count1 = spark.table(test_table).count()
            
            # Second write with different data
            mock_fetch.return_value = sample_brewery_data[:2]  # Less data
            process_bronze_layer(spark, test_date)
            count2 = spark.table(test_table).count()
        
        # Should have replaced the partition
        assert count1 == 3
        assert count2 == 2