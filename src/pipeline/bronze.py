"""Bronze-layer ingestion for street closures CSV and ArcGIS businesses API."""

from __future__ import annotations

import csv
import io
import json
import warnings
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import BronzeConfig


def _extract_attr(attributes: dict[str, Any], keys: list[str]) -> Any:
    """Return first non-null attribute value across candidate keys."""
    for key in keys:
        if key in attributes and attributes.get(key) is not None:
            return attributes.get(key)
    return None


def _ensure_database(spark: SparkSession, table_name: str) -> None:
    """Create database when a fully qualified table name is used."""
    if "." not in table_name:
        return
    database_name = table_name.split(".", maxsplit=1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")


def _write_delta_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    table_path: str | None = None,
) -> None:
    """Write a DataFrame as a Delta table with optional managed path."""
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if table_path:
        writer = writer.option("path", table_path)
    writer.saveAsTable(table_name)


def _validate_row_count(df: DataFrame, layer_name: str, min_rows: int = 1) -> int:
    """Simple row-count validation for data-quality checks."""
    count = df.count()
    if count < min_rows:
        raise ValueError(f"{layer_name} failed row-count validation: {count} < {min_rows}")
    return count


def ingest_street_closures_csv(
    spark: SparkSession,
    csv_url: str,
    output_table: str,
    mode: str = "overwrite",
    output_path: str | None = None,
) -> DataFrame:
    """
    Ingest raw closures CSV into a Bronze Delta table.

    Keeps raw values as strings and appends ingestion metadata.
    """
    _ensure_database(spark, output_table)

    if csv_url.lower().startswith(("http://", "https://")):
        # Spark Connect serverless may not support reading HTTPS URLs as a filesystem.
        response = requests.get(csv_url, timeout=180)
        response.raise_for_status()
        decoded_text = response.content.decode("utf-8-sig", errors="replace")
        rows = list(csv.DictReader(io.StringIO(decoded_text)))
        df = spark.createDataFrame(rows).withColumn("bronze_ingested_at_utc", F.current_timestamp()).withColumn(
            "bronze_source_url", F.lit(csv_url)
        )
    else:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .option("escape", '"')
            .csv(csv_url)
            .withColumn("bronze_ingested_at_utc", F.current_timestamp())
            .withColumn("bronze_source_url", F.lit(csv_url))
        )
    _validate_row_count(df, "bronze_street_closures")

    _write_delta_table(df=df, table_name=output_table, mode=mode, table_path=output_path)
    return df


def _merge_url_query(url: str, extra_params: dict[str, Any]) -> str:
    """Merge query params into an existing URL."""
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({k: str(v) for k, v in extra_params.items() if v is not None})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def fetch_arcgis_pages(
    api_url: str,
    page_size: int,
    timeout_seconds: int,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch all ArcGIS pages using resultOffset pagination.

    Stops when no features are returned, or when the endpoint indicates the transfer
    limit is not exceeded and returned feature count is smaller than page size.

    extra_params: merged into every request (e.g. {"returnGeometry": "true"} for layers).
    """
    all_pages: list[dict[str, Any]] = []
    offset = 0
    page_number = 0

    with requests.Session() as session:
        while True:
            request_params = {
                "resultOffset": offset,
                "resultRecordCount": page_size,
                "f": "json",
            }
            if extra_params:
                request_params.update(extra_params)
            page_url = _merge_url_query(api_url, request_params)
            response = session.get(page_url, timeout=timeout_seconds)
            response.raise_for_status()

            payload: dict[str, Any] = response.json()
            if payload.get("error"):
                raise RuntimeError(f"ArcGIS API error at offset {offset}: {payload['error']}")

            features = payload.get("features", []) or []
            exceeded_limit = bool(payload.get("exceededTransferLimit", False))

            all_pages.append(
                {
                    "page_number": page_number,
                    "result_offset": offset,
                    "feature_count": len(features),
                    "exceeded_transfer_limit": exceeded_limit,
                    "payload_json": json.dumps(payload, ensure_ascii=True),
                    "features": features,
                }
            )

            if not features:
                break

            offset += len(features)
            page_number += 1
            if len(features) < page_size and not exceeded_limit:
                break

    return all_pages


def _build_pages_df(spark: SparkSession, pages: list[dict[str, Any]], source_url: str) -> DataFrame:
    ingested_at = datetime.now(timezone.utc)
    rows = [
        Row(
            page_number=int(page["page_number"]),
            result_offset=int(page["result_offset"]),
            feature_count=int(page["feature_count"]),
            exceeded_transfer_limit=bool(page["exceeded_transfer_limit"]),
            payload_json=str(page["payload_json"]),
            bronze_source_url=source_url,
            bronze_ingested_at_utc=ingested_at,
        )
        for page in pages
    ]

    schema = StructType(
        [
            StructField("page_number", IntegerType(), False),
            StructField("result_offset", IntegerType(), False),
            StructField("feature_count", IntegerType(), False),
            StructField("exceeded_transfer_limit", BooleanType(), False),
            StructField("payload_json", StringType(), False),
            StructField("bronze_source_url", StringType(), False),
            StructField("bronze_ingested_at_utc", TimestampType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def _build_features_df(spark: SparkSession, pages: list[dict[str, Any]], source_url: str) -> DataFrame:
    ingested_at = datetime.now(timezone.utc)
    feature_rows: list[Row] = []
    global_index = 0

    for page in pages:
        for feature in page.get("features", []) or []:
            feature_rows.append(
                Row(
                    feature_index=global_index,
                    page_number=int(page["page_number"]),
                    result_offset=int(page["result_offset"]),
                    attributes_json=json.dumps(feature.get("attributes", {}), ensure_ascii=True),
                    geometry_json=json.dumps(feature.get("geometry", {}), ensure_ascii=True),
                    feature_json=json.dumps(feature, ensure_ascii=True),
                    bronze_source_url=source_url,
                    bronze_ingested_at_utc=ingested_at,
                )
            )
            global_index += 1

    schema = StructType(
        [
            StructField("feature_index", LongType(), False),
            StructField("page_number", IntegerType(), False),
            StructField("result_offset", IntegerType(), False),
            StructField("attributes_json", StringType(), False),
            StructField("geometry_json", StringType(), False),
            StructField("feature_json", StringType(), False),
            StructField("bronze_source_url", StringType(), False),
            StructField("bronze_ingested_at_utc", TimestampType(), False),
        ]
    )
    return spark.createDataFrame(feature_rows, schema=schema)


def ingest_businesses_arcgis(
    spark: SparkSession,
    api_url: str,
    features_output_table: str,
    pages_output_table: str,
    page_size: int,
    timeout_seconds: int,
    mode: str = "overwrite",
    features_output_path: str | None = None,
    pages_output_path: str | None = None,
) -> DataFrame:
    """
    Ingest ArcGIS businesses source into Bronze Delta tables.

    Writes only the features table (page-level table is no longer written).
    """
    _ensure_database(spark, features_output_table)

    # Request WGS84 so business coordinates match segment CRS (segments use outSR=4326)
    pages = fetch_arcgis_pages(
        api_url=api_url,
        page_size=page_size,
        timeout_seconds=timeout_seconds,
        extra_params={"outSR": "4326"},
    )

    features_df = _build_features_df(spark=spark, pages=pages, source_url=api_url)
    _validate_row_count(features_df, "bronze_businesses_features")

    _write_delta_table(
        df=features_df,
        table_name=features_output_table,
        mode=mode,
        table_path=features_output_path,
    )
    return features_df


def _build_street_segments_df(
    spark: SparkSession,
    pages: list[dict[str, Any]],
    source_url: str,
) -> DataFrame:
    ingested_at = datetime.now(timezone.utc)
    rows: list[Row] = []
    global_index = 0

    for page in pages:
        for feature in page.get("features", []) or []:
            attributes = feature.get("attributes", {}) or {}
            # Map API response: each feature has "geometry" when returnGeometry=true (polyline has "paths")
            geometry = (
                feature.get("geometry")
                or feature.get("geom")
                or {}
            )
            if not isinstance(geometry, dict):
                geometry = {}
            paths = geometry.get("paths", []) or []
            k_rechov_raw = _extract_attr(attributes, ["k_rechov", "K_RECHOV", "oid_rechov", "OID_RECHOV"])
            t_shem_rechov_raw = _extract_attr(
                attributes,
                ["t_shem_rechov", "T_SHEM_RECHOV", "t_rechov", "T_RECHOV", "shem_rechov"],
            )
            oid_raw = _extract_attr(attributes, ["oid_rechov", "OID_RECHOV"])
            ms_merechov_raw = _extract_attr(attributes, ["ms_merechov", "MS_MERECHOV"])
            ms_lerechov_raw = _extract_attr(attributes, ["ms_lerechov", "MS_LERECHOV"])
            left_from_raw = _extract_attr(attributes, ["left_from", "LEFT_FROM"])
            left_to_raw = _extract_attr(attributes, ["left_to", "LEFT_TO"])
            right_from_raw = _extract_attr(attributes, ["right_from", "RIGHT_FROM"])
            right_to_raw = _extract_attr(attributes, ["right_to", "RIGHT_TO"])
            rows.append(
                Row(
                    feature_index=global_index,
                    page_number=int(page["page_number"]),
                    result_offset=int(page["result_offset"]),
                    oid_rechov=str(oid_raw) if oid_raw is not None else None,
                    k_rechov=str(k_rechov_raw) if k_rechov_raw is not None else None,
                    t_shem_rechov=str(t_shem_rechov_raw) if t_shem_rechov_raw is not None else None,
                    t_rechov=_extract_attr(attributes, ["t_rechov", "T_RECHOV"]),
                    shem_angli=_extract_attr(attributes, ["shem_angli", "SHEM_ANGLI"]),
                    left_from=str(left_from_raw) if left_from_raw is not None else None,
                    left_to=str(left_to_raw) if left_to_raw is not None else None,
                    right_from=str(right_from_raw) if right_from_raw is not None else None,
                    right_to=str(right_to_raw) if right_to_raw is not None else None,
                    ms_merechov=str(ms_merechov_raw) if ms_merechov_raw is not None else None,
                    ms_lerechov=str(ms_lerechov_raw) if ms_lerechov_raw is not None else None,
                    unique_id=_extract_attr(attributes, ["UniqueId", "uniqueid", "UNIQUEID"]),
                    date_import_raw=_extract_attr(attributes, ["date_import", "DATE_IMPORT"]),
                    shape_length=_extract_attr(attributes, ["Shape_Length", "shape_length", "SHAPE_LENGTH"]),
                    geometry_json=json.dumps(geometry, ensure_ascii=True),
                    paths_json=json.dumps(paths, ensure_ascii=True),
                    attributes_json=json.dumps(attributes, ensure_ascii=True),
                    bronze_source_url=source_url,
                    bronze_ingested_at_utc=ingested_at,
                )
            )
            global_index += 1

    schema = StructType(
        [
            StructField("feature_index", LongType(), False),
            StructField("page_number", IntegerType(), False),
            StructField("result_offset", IntegerType(), False),
            StructField("oid_rechov", StringType(), True),
            StructField("k_rechov", StringType(), True),
            StructField("t_shem_rechov", StringType(), True),
            StructField("t_rechov", StringType(), True),
            StructField("shem_angli", StringType(), True),
            StructField("left_from", StringType(), True),
            StructField("left_to", StringType(), True),
            StructField("right_from", StringType(), True),
            StructField("right_to", StringType(), True),
            StructField("ms_merechov", StringType(), True),
            StructField("ms_lerechov", StringType(), True),
            StructField("unique_id", StringType(), True),
            StructField("date_import_raw", StringType(), True),
            StructField("shape_length", StringType(), True),
            StructField("geometry_json", StringType(), True),
            StructField("paths_json", StringType(), True),
            StructField("attributes_json", StringType(), True),
            StructField("bronze_source_url", StringType(), False),
            StructField("bronze_ingested_at_utc", TimestampType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def ingest_street_segments_arcgis(
    spark: SparkSession,
    api_url: str,
    output_table: str,
    page_size: int,
    timeout_seconds: int,
    mode: str = "overwrite",
    output_path: str | None = None,
) -> DataFrame:
    """Ingest ArcGIS layer 507 street segments (Polyline) into a Bronze Delta table.
    Ensures returnGeometry=true on every request so geometry_json and paths_json are populated."""
    _ensure_database(spark, output_table)
    # Include returnGeometry, outSR, and outFields in every paginated request (required for geometry in response).
    # Layer 507 (Polyline) full city map; outSR=4326 for WGS84 coordinates.
    segment_extra_params = {
        "returnGeometry": "true",
        "outSR": "4326",  # Crucial for WGS84 mapping
        "outFields": "*",
    }
    pages = fetch_arcgis_pages(
        api_url=api_url,
        page_size=page_size,
        timeout_seconds=timeout_seconds,
        extra_params=segment_extra_params,
    )
    segments_df = _build_street_segments_df(spark=spark, pages=pages, source_url=api_url)
    _validate_row_count(segments_df, "bronze_street_segments")
    # Warn if no geometry (pipeline still writes so you can inspect Bronze; Silver/Gold may have 0 segment matches)
    with_geometry = segments_df.filter(
        (F.col("geometry_json").isNotNull())
        & (F.trim(F.col("geometry_json")) != F.lit("{}"))
        & (F.trim(F.col("geometry_json")) != F.lit("[]"))
    )
    geom_count = with_geometry.count()
    if geom_count < 1:
        warnings.warn(
            "bronze_street_segments: no rows with non-empty geometry_json. "
            "Layer 507 may not return geometry (e.g. server ignores returnGeometry). "
            "Writing Bronze anyway; Silver/Gold segment matching will be empty. "
            "Test the URL in a browser with returnGeometry=True and outFields=* to verify."
        )
    _write_delta_table(df=segments_df, table_name=output_table, mode=mode, table_path=output_path)
    return segments_df


def run_bronze_layer(
    spark: SparkSession,
    config: BronzeConfig | None = None,
) -> dict[str, DataFrame]:
    """Run both Bronze ingestions and return loaded DataFrames."""
    cfg = config or BronzeConfig()
    if cfg.write_mode != "overwrite":
        raise ValueError("Bronze layer supports overwrite mode only for idempotency.")

    street_df = ingest_street_closures_csv(
        spark=spark,
        csv_url=cfg.street_closures_csv_url,
        output_table=cfg.bronze_street_table,
        mode=cfg.write_mode,
    )
    features_df = ingest_businesses_arcgis(
        spark=spark,
        api_url=cfg.businesses_arcgis_api_url,
        features_output_table=cfg.bronze_business_features_table,
        pages_output_table=cfg.bronze_business_pages_table,
        page_size=cfg.businesses_page_size,
        timeout_seconds=cfg.arcgis_timeout_seconds,
        mode=cfg.write_mode,
    )
    segments_df = ingest_street_segments_arcgis(
        spark=spark,
        api_url=cfg.street_segments_arcgis_api_url,
        output_table=cfg.bronze_street_segments_table,
        page_size=cfg.street_segments_page_size,
        timeout_seconds=cfg.arcgis_timeout_seconds,
        mode=cfg.write_mode,
    )

    return {
        "street_closures_bronze": street_df,
        "businesses_features_bronze": features_df,
        "street_segments_bronze": segments_df,
    }
