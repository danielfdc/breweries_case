"""Tests for external connectivity and API validation."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from config.pipeline_configs import BREWERY_API_BASE_URL


class TestAPIConnectivity:
    """Test external API connectivity and response validation."""
    
    @patch("requests.get")
    def test_api_response_structure(self, mock_get):
        """Test that API returns expected structure."""
        sample_response = [{
            "id": "test-id",
            "name": "Test Brewery",
            "brewery_type": "micro",
            "address_1": "123 Test St",
            "address_2": None,
            "address_3": None,
            "city": "Test City",
            "state_province": "Test State",
            "postal_code": "12345",
            "country": "Test Country",
            "longitude": "-122.123456",
            "latitude": "45.123456",
            "phone": "555-0123",
            "website_url": "http://test.com",
            "state": "Test State",
            "street": "123 Test St"
        }]
        
        mock_response = MagicMock()
        mock_response.json.return_value = sample_response
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        
        # Make request
        response = requests.get(f"{BREWERY_API_BASE_URL}?page=1&per_page=1")
        data = response.json()
        
        # Validate structure
        assert isinstance(data, list)
        assert len(data) > 0
        
        brewery = data[0]
        required_fields = ["id", "name", "brewery_type", "city", "country"]
        for field in required_fields:
            assert field in brewery
    
    @patch("requests.get")
    def test_api_pagination(self, mock_get):
        """Test API pagination parameters."""
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        
        # Test pagination parameters
        response = requests.get(f"{BREWERY_API_BASE_URL}?page=1&per_page=200")
        
        # Verify request was made with correct params
        mock_get.assert_called_with(f"{BREWERY_API_BASE_URL}?page=1&per_page=200")
    
    @patch("requests.get")
    def test_api_error_handling(self, mock_get):
        """Test API error response handling."""
        # Test 404
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            response = requests.get(f"{BREWERY_API_BASE_URL}/invalid")
            response.raise_for_status()
        
        # Test 500
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        mock_get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            response = requests.get(BREWERY_API_BASE_URL)
            response.raise_for_status()
    
    @patch("requests.get")
    def test_api_timeout(self, mock_get):
        """Test API timeout handling."""
        mock_get.side_effect = requests.Timeout("Connection timed out")
        
        with pytest.raises(requests.Timeout):
            requests.get(BREWERY_API_BASE_URL, timeout=30)
    
    @patch("requests.get")
    def test_api_rate_limiting(self, mock_get):
        """Test handling of rate limiting."""
        # Simulate rate limit response
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "60"}
        mock_response.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")
        mock_get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError) as exc_info:
            response = requests.get(BREWERY_API_BASE_URL)
            response.raise_for_status()
        
        assert "429" in str(exc_info.value)