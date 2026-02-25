"""Pipeline configuration for the Tel Aviv medallion assignment."""

import os
from dataclasses import dataclass


STREET_CLOSURES_CSV_URL = "https://storage.googleapis.com/test_onedatai/rechov_sagur.csv"
BUSINESSES_ARCGIS_API_URL = (
    "https://gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/925/query"
    "?where=1%3D1&outFields=*&f=json"
)
STREET_SEGMENTS_ARCGIS_API_URL = (
    "https://gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/507/query"
    "?where=1%3D1&outFields=*&returnGeometry=true&f=json"
)

# ArcGIS pagination controls.
ARCGIS_PAGE_SIZE = 2000
ARCGIS_REQUEST_TIMEOUT_SECONDS = 180

# Default database and Delta table names.
DEFAULT_DATABASE = "medallion"
DEFAULT_WRITE_MODE = "overwrite"

BRONZE_STREET_CLOSURES_TABLE = f"{DEFAULT_DATABASE}.bronze_street_closures"
BRONZE_BUSINESSES_FEATURES_TABLE = f"{DEFAULT_DATABASE}.bronze_businesses_features"
BRONZE_BUSINESSES_PAGES_TABLE = f"{DEFAULT_DATABASE}.bronze_businesses_pages"
BRONZE_STREET_SEGMENTS_TABLE = f"{DEFAULT_DATABASE}.bronze_street_segments"

SILVER_STREET_CLOSURES_DAILY_TABLE = f"{DEFAULT_DATABASE}.silver_street_closures_daily"
SILVER_BUSINESSES_TABLE = f"{DEFAULT_DATABASE}.silver_businesses"
SILVER_STREET_SEGMENTS_TABLE = f"{DEFAULT_DATABASE}.silver_street_segments"

GOLD_DAILY_COMPENSATION_TABLE = f"{DEFAULT_DATABASE}.gold_business_daily_compensation_2023"
GOLD_BUSINESS_ANNUAL_TABLE = f"{DEFAULT_DATABASE}.gold_business_annual_compensation_2023"
GOLD_STREET_ANNUAL_TABLE = f"{DEFAULT_DATABASE}.gold_street_annual_compensation_2023"
GOLD_UNMATCHED_CLOSURES_AUDIT_TABLE = f"{DEFAULT_DATABASE}.gold_unmatched_closures_audit_2023"
GOLD_DASHBOARD_METRICS_TABLE = f"{DEFAULT_DATABASE}.gold_dashboard_metrics_2023"
GOLD_SEGMENT_MATCH_QA_TABLE = f"{DEFAULT_DATABASE}.gold_segment_match_qa_2023"
GOLD_PAYOUT_ASSIGNMENT_TABLE = f"{DEFAULT_DATABASE}.gold_payout_assignment_2023"
GOLD_PAYOUT_PRECISION_TABLE = f"{DEFAULT_DATABASE}.gold_payout_precision_2023"
GOLD_AUDIT_ASSIGNMENT_UNMATCHED_TABLE = f"{DEFAULT_DATABASE}.gold_audit_assignment_unmatched"
GOLD_AUDIT_PRECISION_UNMATCHED_TABLE = f"{DEFAULT_DATABASE}.gold_audit_precision_unmatched"

COMPENSATION_YEAR = 2023
DAILY_COMPENSATION_RATE_ILS = 100.0
DAILY_COMPENSATION_CAP_ILS = 10000.0
MAX_SEGMENT_DISTANCE_M = 400.0
MAX_SEGMENT_FALLBACK_DISTANCE_M = 1000.0
LONG_SEGMENT_THRESHOLD_M = 300.0


@dataclass(frozen=True)
class BronzeConfig:
    """Runtime configuration for Bronze-layer ingestion."""

    street_closures_csv_url: str = STREET_CLOSURES_CSV_URL
    businesses_arcgis_api_url: str = BUSINESSES_ARCGIS_API_URL
    street_segments_arcgis_api_url: str = STREET_SEGMENTS_ARCGIS_API_URL
    businesses_page_size: int = ARCGIS_PAGE_SIZE
    street_segments_page_size: int = ARCGIS_PAGE_SIZE
    arcgis_timeout_seconds: int = ARCGIS_REQUEST_TIMEOUT_SECONDS

    bronze_street_table: str = BRONZE_STREET_CLOSURES_TABLE
    bronze_business_features_table: str = BRONZE_BUSINESSES_FEATURES_TABLE
    bronze_business_pages_table: str = BRONZE_BUSINESSES_PAGES_TABLE
    bronze_street_segments_table: str = BRONZE_STREET_SEGMENTS_TABLE
    write_mode: str = DEFAULT_WRITE_MODE


@dataclass(frozen=True)
class SilverConfig:
    """Runtime configuration for Silver-layer transformations."""

    bronze_street_table: str = BRONZE_STREET_CLOSURES_TABLE
    bronze_business_features_table: str = BRONZE_BUSINESSES_FEATURES_TABLE
    bronze_street_segments_table: str = BRONZE_STREET_SEGMENTS_TABLE

    silver_street_daily_table: str = SILVER_STREET_CLOSURES_DAILY_TABLE
    silver_businesses_table: str = SILVER_BUSINESSES_TABLE
    silver_street_segments_table: str = SILVER_STREET_SEGMENTS_TABLE
    max_segment_distance_m: float = MAX_SEGMENT_DISTANCE_M
    max_segment_fallback_distance_m: float = MAX_SEGMENT_FALLBACK_DISTANCE_M
    long_segment_threshold_m: float = LONG_SEGMENT_THRESHOLD_M
    write_mode: str = DEFAULT_WRITE_MODE


@dataclass(frozen=True)
class GoldConfig:
    """Runtime configuration for Gold-layer compensation outputs."""

    silver_street_daily_table: str = SILVER_STREET_CLOSURES_DAILY_TABLE
    silver_businesses_table: str = SILVER_BUSINESSES_TABLE
    silver_street_segments_table: str = SILVER_STREET_SEGMENTS_TABLE

    gold_daily_compensation_table: str = GOLD_DAILY_COMPENSATION_TABLE
    gold_business_annual_table: str = GOLD_BUSINESS_ANNUAL_TABLE
    gold_street_annual_table: str = GOLD_STREET_ANNUAL_TABLE
    gold_unmatched_closures_audit_table: str = GOLD_UNMATCHED_CLOSURES_AUDIT_TABLE
    gold_dashboard_metrics_table: str = GOLD_DASHBOARD_METRICS_TABLE
    gold_segment_match_qa_table: str = GOLD_SEGMENT_MATCH_QA_TABLE
    gold_payout_assignment_table: str = GOLD_PAYOUT_ASSIGNMENT_TABLE
    gold_payout_precision_table: str = GOLD_PAYOUT_PRECISION_TABLE
    gold_audit_assignment_unmatched_table: str = GOLD_AUDIT_ASSIGNMENT_UNMATCHED_TABLE
    gold_audit_precision_unmatched_table: str = GOLD_AUDIT_PRECISION_UNMATCHED_TABLE

    compensation_year: int = COMPENSATION_YEAR
    daily_compensation_rate_ils: float = DAILY_COMPENSATION_RATE_ILS
    daily_compensation_cap_ils: float = DAILY_COMPENSATION_CAP_ILS
    max_segment_distance_m: float = MAX_SEGMENT_DISTANCE_M
    write_mode: str = DEFAULT_WRITE_MODE


@dataclass(frozen=True)
class SnowflakeConfig:
    """Configuration for Snowflake export destination.
    Credentials from os.environ: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD.
    Optional: SNOWFLAKE_ACCOUNT to override account (e.g. if 404 on login).
    Account format: orgname-accountname or legacy locator.region.cloud
    (e.g. iz00241.eu-central-2.aws or xy12345.us-east-1.aws).
    """

    account: str = "iz00241.eu-central-2.aws"
    user: str = ""
    password: str = ""
    warehouse: str = "TEL_AVIV_WH"
    database: str = "TEL_AVIV_PROJECT"
    schema: str = "GOLD"


def snowflake_config_from_env() -> SnowflakeConfig | None:
    """Build SnowflakeConfig from environment variables. Returns None if credentials missing."""
    user = os.environ.get("SNOWFLAKE_USER", "")
    password = os.environ.get("SNOWFLAKE_PASSWORD", "")
    if not user or not password:
        return None
    account = os.environ.get("SNOWFLAKE_ACCOUNT", "iz00241.eu-central-2.aws")
    return SnowflakeConfig(account=account, user=user, password=password)
