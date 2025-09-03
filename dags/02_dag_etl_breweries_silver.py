"""Airflow DAG for Silver layer ELT pipeline."""
from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from pyspark.sql import SparkSession

import sys
sys.path.insert(0, '/opt/airflow/src')

from config.breweries_pipeline_configs import (
    DEFAULT_ARGS,
    SCHEDULE_INTERVAL,
    SPARK_APP_NAME_SILVER,
)
from processors.breweries_silver_processors import process_silver_layer

logger = logging.getLogger(__name__)


def run_silver_etl(**context) -> None:
    """Execute silver layer ETL process."""
    execution_date = context["ds"]
    logger.info(f"Starting Silver ETL for date: {execution_date}")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME_SILVER) \
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
        process_silver_layer(spark, extraction_date)
        logger.info("Silver ETL completed successfully")
    finally:
        spark.stop()


# Define DAG
dag = DAG(
    "02_breweries_silver_elt",
    default_args=DEFAULT_ARGS,
    description="ELT pipeline for breweries silver layer",
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=["breweries", "silver", "ett", "transform"],
)

# Wait for bronze layer to complete
wait_for_bronze = ExternalTaskSensor(
    task_id="wait_for_bronze",
    external_dag_id="01_breweries_bronze_elt",
    external_task_id="process_bronze_layer",
    dag=dag,
    mode="reschedule",
    poke_interval=60,
)

# Define task
silver_task = PythonOperator(
    task_id="process_silver_layer",
    python_callable=run_silver_etl,
    dag=dag,
)

wait_for_bronze >> silver_task