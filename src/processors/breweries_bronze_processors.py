"""Bronze layer processor for brewery data."""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType

from config.breweries_pipeline_configs import (
    BREWERY_API_BASE_URL,
    BREWERY_API_MAX_PAGES,
    BREWERY_API_PAGE_SIZE,
    BRONZE_TABLE,
)

logger = logging.getLogger(__name__)


def _session_with_retry(
    total_retries: int = 5,
    backoff_factor: float = 0.6,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
    allowed_methods: frozenset[str] = frozenset({"GET"}),
    user_agent: str = "brew-fetcher/1.0 (+https://example.com)"
) -> requests.Session:
    """
    Cria uma requests.Session com política de retry robusta:
    - Exponential backoff (controlado por backoff_factor)
    - Respeita cabeçalho Retry-After (429/5xx)
    - Re-tenta em erros de conexão/leitura e em respostas 429/5xx
    """
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods,
        raise_on_status=False,             # não lança durante os retries
        respect_retry_after_header=True,   # respeita Retry-After
    )
    adapter = HTTPAdapter(max_retries=retry)

    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def fetch_breweries_from_api(session: Optional[requests.Session] = None) -> list[dict[str, Any]]:
    """
    Busca todas as breweries paginando, com retries e backoff exponencial.
    - Falhas transitórias (rede/429/5xx) são re-tentadas automaticamente.
    - Exceções originais são preservadas (stack trace limpo).
    - Log detalhado por página + total final.

    Paradas:
    - Página vazia -> fim.
    - Página parcial (< PAGE_SIZE) -> última página.
    - Máximo de páginas (BREWERY_API_MAX_PAGES) -> segurança.
    """
    all_breweries: list[dict[str, Any]] = []
    page = 1
    own_session = session is None
    s = session or _session_with_retry()

    try:
        while page <= BREWERY_API_MAX_PAGES:
            url = f"{BREWERY_API_BASE_URL}?page={page}&per_page={BREWERY_API_PAGE_SIZE}"
            logger.info("Fetching page %s from API", page)

            # timeout=(connect_timeout, read_timeout)
            resp = s.get(url, timeout=(5, 30))

            # Após esgotar retries, se ainda vier 4xx/5xx, agora sim levantamos
            if resp.status_code >= 400:
                # Pequena ajuda de diagnóstico (sem despejar payload gigante)
                preview = resp.text[:300].replace("\n", " ")
                logger.error("HTTP %s on page %s. Body preview: %s", resp.status_code, page, preview)
                resp.raise_for_status()

            try:
                breweries = resp.json()
            except ValueError as e:
                logger.error("Invalid JSON on page %s", page)
                raise RuntimeError("API returned invalid JSON") from e

            if not breweries:
                logger.info("Empty page %s returned. No more data.", page)
                break

            if not isinstance(breweries, list):
                logger.error("Unexpected payload type on page %s: %s", page, type(breweries).__name__)
                raise TypeError("Unexpected API payload type (expected a list)")

            all_breweries.extend(breweries)
            logger.info("Fetched %d breweries from page %s", len(breweries), page)

            # Última página detectada pela quantidade
            if len(breweries) < BREWERY_API_PAGE_SIZE:
                logger.info("Page %s returned less than page size. Stopping.", page)
                break

            page += 1

    except requests.RequestException:
        # Mantém stack trace original e adiciona contexto rico no log
        logger.exception("Network/HTTP error while fetching breweries (page %s).", page)
        raise
    finally:
        # Fecha apenas se a sessão foi criada aqui
        if own_session:
            s.close()

    logger.info("Total breweries fetched: %d", len(all_breweries))
    return all_breweries


def process_bronze_layer(spark: SparkSession, extraction_date: date) -> None:
    """Process and persist raw brewery data to bronze layer."""
    logger.info(f"Starting bronze layer processing for {extraction_date}")
    
    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # Fetch data from API
    breweries = fetch_breweries_from_api()
    
    # Define schema for bronze layer
    schema = StructType([
        StructField("raw_json", StringType(), False),
        StructField("extraction_date", DateType(), False)
    ])
    
    # Convert to DataFrame with raw JSON
    data = [(json.dumps(brewery), extraction_date) for brewery in breweries]
    df_bronze_output = spark.createDataFrame(data, schema)
    
    # Write to bronze table with partition overwrite
    df_bronze_output.write \
        .mode("overwrite") \
        .partitionBy("extraction_date") \
        .format("iceberg") \
        .saveAsTable(BRONZE_TABLE)
    
    row_count = df_bronze_output.count()
    logger.info(f"Bronze layer completed: {row_count} records written for {extraction_date}")