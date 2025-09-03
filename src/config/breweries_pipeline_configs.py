"""Pipeline configuration constants."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

# API Configuration
BREWERY_API_BASE_URL: Final[str] = "https://api.openbrewerydb.org/v1/breweries"
BREWERY_API_PAGE_SIZE: Final[int] = 200  # Max allowed by API
BREWERY_API_MAX_PAGES: Final[int] = 50  # Limit for safety

# Table Names
BRONZE_TABLE: Final[str] = "nessie.bronze_layer.tbl_bronze_breweries"
SILVER_TABLE: Final[str] = "nessie.silver_layer.tbl_silver_brewery"
GOLD_TABLE: Final[str] = "nessie.gold_layer.tbl_gold_brewery_agg"

# Spark Configuration
SPARK_APP_NAME_BRONZE: Final[str] = "BreweriesBronzeETL"
SPARK_APP_NAME_SILVER: Final[str] = "BreweriesSilverETL"
SPARK_APP_NAME_GOLD: Final[str] = "BreweriesGoldETL"

# Airflow Configuration
DEFAULT_ARGS: Final[dict] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

SCHEDULE_INTERVAL: Final[str] = "@daily"