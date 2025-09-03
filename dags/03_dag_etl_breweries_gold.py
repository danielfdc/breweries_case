"""Airflow DAG for Gold layer ETL pipeline."""
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
    SPARK_APP_NAME_GOLD,
)
from processors.breweries_gold_processors import process_gold_layer

logger = logging.getLogger(__name__)


def run_gold_etl(**context) -> None:
    """Execute gold layer ETL process."""
    execution_date = context["ds"]
    logger.info(f"Starting Gold ETL for date: {execution_date}")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME_GOLD) \
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
        process_gold_layer(spark, extraction_date)
        logger.info("Gold ETL completed successfully")
    finally:
        spark.stop()


# Define DAG
dag = DAG(
    "03_breweries_gold_etl",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline for breweries gold layer",
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=["breweries", "gold", "etl", "summarize"],
)

# Wait for silver layer to complete
wait_for_silver = ExternalTaskSensor(
    task_id="wait_for_silver",
    external_dag_id="02_breweries_silver_elt",
    external_task_id="process_silver_layer",
    dag=dag,
    mode="reschedule",
    poke_interval=60,
)

# Define task
gold_task = PythonOperator(
    task_id="process_gold_layer",
    python_callable=run_gold_etl,
    dag=dag,
)

wait_for_silver >> gold_task