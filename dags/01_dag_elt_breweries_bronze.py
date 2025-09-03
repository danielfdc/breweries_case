"""Airflow DAG for Bronze layer ELT pipeline."""
from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

import sys
sys.path.insert(0, '/opt/airflow/src')

from config.breweries_pipeline_configs import (
    DEFAULT_ARGS,
    SCHEDULE_INTERVAL,
    SPARK_APP_NAME_BRONZE,
)
from processors.breweries_bronze_processors import process_bronze_layer

logger = logging.getLogger(__name__)


def run_bronze_etl(**context) -> None:
    """Execute bronze layer ETL process."""
    execution_date = context["ds"]
    logger.info(f"Starting Bronze ETL for date: {execution_date}")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME_BRONZE) \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    try:
        extraction_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
        process_bronze_layer(spark, extraction_date)
        logger.info("Bronze ETL completed successfully")
    finally:
        spark.stop()


# Define DAG
dag = DAG(
    "01_breweries_bronze_elt",
    default_args=DEFAULT_ARGS,
    description="ELT pipeline for breweries bronze layer",
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=["breweries", "bronze", "elt"],
)

# Define task
bronze_task = PythonOperator(
    task_id="process_bronze_layer",
    python_callable=run_bronze_etl,
    dag=dag,
)

bronze_task