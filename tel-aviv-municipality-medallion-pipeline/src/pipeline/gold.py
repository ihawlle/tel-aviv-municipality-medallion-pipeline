"""Gold-layer compensation outputs for 2023."""

from __future__ import annotations

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import GoldConfig, SnowflakeConfig

# Default audit table names (used when GoldConfig has no audit table attributes, e.g. older deployed config).
_DEFAULT_AUDIT_ASSIGNMENT_UNMATCHED_TABLE = "medallion.gold_audit_assignment_unmatched"
_DEFAULT_AUDIT_PRECISION_UNMATCHED_TABLE = "medallion.gold_audit_precision_unmatched"


def export_to_snowflake(
    df: DataFrame,
    table_name: str,
    cfg: SnowflakeConfig,
) -> None:
    """Export DataFrame to Snowflake table. Overwrite mode for idempotency.
    Uses native Python Snowflake connector (Databricks Serverless compatible).
    Skips export when credentials are missing (no-op for local tests).
    """
    if not cfg.user or not cfg.password:
        return

    pandas_df = df.toPandas()

    conn = snowflake.connector.connect(
        account=cfg.account,
        user=cfg.user,
        password=cfg.password,
        warehouse=cfg.warehouse,
        database=cfg.database,
        schema=cfg.schema,
    )
    try:
        # Drop table first so schema changes (e.g. new columns) are applied.
        # write_pandas(overwrite=True) truncates but does not alter schema.
        full_name = f'{cfg.database}.{cfg.schema}.{table_name}'
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{cfg.database}"."{cfg.schema}"."{table_name}"')
        cur.close()
        print(f"Snowflake: dropped (if existed) and recreating {full_name} with {list(pandas_df.columns)}")
        write_pandas(
            conn=conn,
            df=pandas_df,
            table_name=table_name,
            database=cfg.database,
            schema=cfg.schema,
            overwrite=True,
            auto_create_table=True,
        )
    finally:
        conn.close()


def _is_municipal(holder_name_col: Column) -> Column:
    """True if holder_name contains municipality strings (exclude from external payout tables)."""
    name = F.coalesce(holder_name_col.cast("string"), F.lit(""))
    return name.contains("עיריית תל אביב") | name.contains("עיריית ת\"א") | name.contains("עיריית ת''א")


def _audit_reason_column(
    street_name_col: Column,
    affected_date_col: Column,
    street_signature_col: Column,
    businesses_on_street_col: Column,
    match_count_col: Column,
    compensation_year: int,
    include_outside_spatial: bool = False,
) -> Column:
    """Build audit_reason Column for assignment/precision audit tables."""
    base = (
        F.when(
            street_name_col.isNull()
            | (F.trim(F.coalesce(street_name_col, F.lit(""))) == F.lit(""))
            | (F.trim(F.coalesce(street_name_col, F.lit(""))) == F.lit("0")),
            F.lit("INVALID_SOURCE_DATA"),
        )
        .when(F.year(affected_date_col) != F.lit(compensation_year), F.lit("OUT_OF_DATE_RANGE"))
        .when(
            businesses_on_street_col.isNull()
            & street_signature_col.isNotNull()
            & (F.trim(street_signature_col) != F.lit("")),
            F.lit("STREET_NOT_FOUND_IN_BUSINESS_DB"),
        )
    )
    if include_outside_spatial:
        base = base.when(
            (businesses_on_street_col > 0) & (match_count_col == 0),
            F.lit("OUTSIDE_SPATIAL_IMPACT_ZONE"),
        )
    return (
        base.when(match_count_col == 0, F.lit("STREET_NOT_FOUND_IN_BUSINESS_DB"))
        .otherwise(F.lit("MATCHED_STREET_BUT_NO_ELIGIBLE_BUSINESSES"))
    )


def _ensure_database(spark: SparkSession, table_name: str) -> None:
    """Create database when a fully qualified table name is used."""
    if "." not in table_name:
        return
    database_name = table_name.split(".", maxsplit=1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")


def _write_delta_table_overwrite(
    df: DataFrame,
    table_name: str,
    table_path: str | None = None,
) -> None:
    """Write a DataFrame as an idempotent Delta overwrite."""
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if table_path:
        writer = writer.option("path", table_path)
    writer.saveAsTable(table_name)


def _validate_row_count(df: DataFrame, layer_name: str, min_rows: int = 1) -> int:
    """Simple row-count validation for data-quality checks."""
    count = df.count()
    if count < min_rows:
        raise ValueError(f"{layer_name} failed row-count validation: {count} < {min_rows}")
    return count


def _validate_payout_sanity(
    df: DataFrame,
    layer_name: str,
    max_total_ils: float = 100_000_000.0,
    max_rows: int | None = None,
) -> None:
    """Validate payout table is within sane bounds; warn (do not raise) if sum or row count exceeds limit."""
    import warnings

    total_ils = df.agg(F.sum("payout_ils")).collect()[0][0]
    total_ils = float(total_ils) if total_ils is not None else 0.0
    if total_ils > max_total_ils:
        warnings.warn(
            f"{layer_name} total payout {total_ils:,.0f} ILS exceeds max {max_total_ils:,.0f} ILS; "
            "possible data explosion (cartesian product).",
            UserWarning,
            stacklevel=2,
        )
    if max_rows is not None:
        row_count = df.count()
        if row_count > max_rows:
            warnings.warn(
                f"{layer_name} row count {row_count} exceeds expected max {max_rows}; "
                "possible data explosion.",
                UserWarning,
                stacklevel=2,
            )


def _build_impacted_closure_segments(
    silver_street_daily_df: DataFrame,
    silver_segments_df: DataFrame,
    compensation_year: int,
) -> DataFrame:
    """Build closure-day to impacted-segment mapping using street chain spatial order.
    Considers both streets per closure-day (from and to) so both get segment matches."""
    closures_df = (
        silver_street_daily_df.filter(F.year(F.col("affected_date")) == F.lit(compensation_year))
        .select(
            F.col("closure_id"),
            F.col("affected_date"),
            F.col("closure_start_ts"),
            F.col("closure_end_ts"),
            F.when(F.col("street_role") == "from", F.col("main_street_id")).otherwise(F.lit(None)).alias("main_street_id"),
            F.col("street_signature").alias("main_street_signature"),
            F.col("street_name_raw").alias("main_street_name_raw"),
            F.col("start_cross_street_id"),
            F.col("end_cross_street_id"),
        )
        .filter(F.col("main_street_signature").isNotNull())
        .filter(F.trim(F.coalesce(F.col("main_street_signature"), F.lit(""))) != "")
        .dropDuplicates(["closure_id", "affected_date", "main_street_signature"])
    )

    candidate_segments_df = (
        closures_df.alias("c")
        .join(
            silver_segments_df.alias("s"),
            on=(
            (
                F.col("c.main_street_id").isNotNull()
                    & (F.col("c.main_street_id").cast("string") == F.col("s.k_rechov"))
                )
                | (
                    F.col("c.main_street_id").isNull()
                    & (F.col("c.main_street_signature") == F.col("s.street_signature"))
                )
            ),
            how="inner",
        )
        .select(
            F.col("c.*"),
            F.col("s.segment_id"),
            F.col("s.segment_mid_x"),
            F.col("s.segment_mid_y"),
            F.col("s.ms_merechov"),
            F.col("s.ms_lerechov"),
        )
    )

    axis_stats_df = candidate_segments_df.groupBy("closure_id", "affected_date", "main_street_signature").agg(
        (F.max("segment_mid_x") - F.min("segment_mid_x")).alias("x_range"),
        (F.max("segment_mid_y") - F.min("segment_mid_y")).alias("y_range"),
    )

    ordered_df = (
        candidate_segments_df.join(
            axis_stats_df,
            on=["closure_id", "affected_date", "main_street_signature"],
            how="left",
        )
        .withColumn(
            "segment_order_key",
            F.when(F.col("x_range") >= F.col("y_range"), F.col("segment_mid_x")).otherwise(F.col("segment_mid_y")),
        )
        .withColumn(
            "start_hit",
            (F.col("start_cross_street_id").isNotNull())
            & (
                (F.col("ms_merechov") == F.col("start_cross_street_id"))
                | (F.col("ms_lerechov") == F.col("start_cross_street_id"))
            ),
        )
        .withColumn(
            "end_hit",
            (F.col("end_cross_street_id").isNotNull())
            & (
                (F.col("ms_merechov") == F.col("end_cross_street_id"))
                | (F.col("ms_lerechov") == F.col("end_cross_street_id"))
            ),
        )
    )

    anchor_df = ordered_df.groupBy("closure_id", "affected_date", "main_street_signature").agg(
        F.min(F.when(F.col("start_hit"), F.col("segment_order_key"))).alias("start_anchor"),
        F.min(F.when(F.col("end_hit"), F.col("segment_order_key"))).alias("end_anchor"),
    )

    impacted_df = (
        ordered_df.join(
            anchor_df,
            on=["closure_id", "affected_date", "main_street_signature"],
            how="left",
        )
        .withColumn(
            "is_in_spatial_chain",
            F.when(
                F.col("start_anchor").isNotNull() & F.col("end_anchor").isNotNull(),
                F.col("segment_order_key").between(
                    F.least(F.col("start_anchor"), F.col("end_anchor")),
                    F.greatest(F.col("start_anchor"), F.col("end_anchor")),
                ),
            ).otherwise(F.lit(True)),
        )
        .filter(F.col("is_in_spatial_chain"))
        .select(
            "closure_id",
            "affected_date",
            "closure_start_ts",
            "closure_end_ts",
            "main_street_id",
            "main_street_signature",
            "main_street_name_raw",
            "segment_id",
            "segment_mid_x",
            "segment_mid_y",
        )
        .dropDuplicates(["closure_id", "affected_date", "segment_id"])
    )
    # Fallback: if chain anchors are missing for a closure-day street, use all segments on that street.
    closures_2023_df = (
        silver_street_daily_df.filter(F.year(F.col("affected_date")) == F.lit(compensation_year))
        .select(
            F.col("closure_id"),
            F.col("affected_date"),
            F.when(F.col("street_role") == "from", F.col("main_street_id")).otherwise(F.lit(None)).alias("main_street_id"),
            F.col("street_signature").alias("main_street_signature"),
            F.col("street_name_raw").alias("main_street_name_raw"),
            F.col("closure_start_ts"),
            F.col("closure_end_ts"),
        )
        .filter(F.col("main_street_signature").isNotNull())
        .filter(F.trim(F.coalesce(F.col("main_street_signature"), F.lit(""))) != "")
        .dropDuplicates(["closure_id", "affected_date", "main_street_signature"])
    )
    impacted_keys_df = impacted_df.select("closure_id", "affected_date", "main_street_signature").dropDuplicates()
    missing_chain_df = closures_2023_df.alias("c").join(
        impacted_keys_df.alias("i"),
        on=["closure_id", "affected_date", "main_street_signature"],
        how="left_anti",
    )
    fallback_df = (
        missing_chain_df.alias("c")
        .join(
            silver_segments_df.alias("s"),
            on=(
            (
                F.col("c.main_street_id").isNotNull()
                    & (F.col("c.main_street_id").cast("string") == F.col("s.k_rechov"))
                )
                | (
                    F.col("c.main_street_id").isNull()
                    & (F.col("c.main_street_signature") == F.col("s.street_signature"))
                )
            ),
            how="inner",
        )
        .select(
            F.col("c.closure_id"),
            F.col("c.affected_date"),
            F.col("c.closure_start_ts"),
            F.col("c.closure_end_ts"),
            F.col("c.main_street_id"),
            F.col("c.main_street_signature"),
            F.col("c.main_street_name_raw"),
            F.col("s.segment_id"),
            F.col("s.segment_mid_x"),
            F.col("s.segment_mid_y"),
        )
        .dropDuplicates(["closure_id", "affected_date", "segment_id"])
    )
    return impacted_df.unionByName(fallback_df).dropDuplicates(["closure_id", "affected_date", "segment_id"])


def _build_gold_daily_compensation(
    impacted_segments_df: DataFrame,
    silver_businesses_df: DataFrame,
    compensation_year: int,
    daily_rate_ils: float,
    daily_cap_ils: float,
) -> DataFrame:
    joined_df = (
        impacted_segments_df.withColumn("segment_id_str", F.col("segment_id").cast("string")).alias("i")
        .join(
            silver_businesses_df.withColumn("assigned_segment_id_str", F.col("assigned_segment_id").cast("string")).alias("b"),
            on=F.col("i.segment_id_str") == F.col("b.assigned_segment_id_str"),
            how="inner",
        )
        .select(
            F.col("i.affected_date").alias("compensation_date"),
            F.col("i.main_street_signature").alias("street_signature"),
            F.col("i.main_street_name_raw").alias("closure_street_name_raw"),
            F.col("i.closure_id"),
            F.col("b.business_id"),
            F.col("b.street_name_raw").alias("business_street_name_raw"),
            F.col("b.house_number"),
            F.col("b.business_usage"),
            F.col("b.shetach"),
            F.col("b.x_coord"),
            F.col("b.y_coord"),
            F.col("b.geometry_json"),
            F.col("b.assigned_segment_id"),
            F.col("b.segment_distance_m"),
        )
    )

    # Compensation is once per business per day, independent of number of closures.
    daily_unique_df = joined_df.dropDuplicates(["business_id", "compensation_date"])

    return daily_unique_df.withColumn(
        "daily_compensation_ils",
        F.least(
            F.lit(daily_cap_ils),
            F.greatest(F.lit(0.0), F.col("shetach")) * F.lit(daily_rate_ils),
        ),
    )


def _build_gold_payout_assignment_2023(
    silver_street_daily_df: DataFrame,
    silver_businesses_df: DataFrame,
    compensation_year: int,
    daily_cap_ils: float,
) -> DataFrame:
    """Build gold_payout_assignment_2023: hybrid match on street ID (primary) or street_signature (fallback);
    one payout per business per day; when multiple closures on same day, keep the closure with longest duration."""
    closure_2023_df = (
        silver_street_daily_df.filter(
            F.year(F.col("affected_date")) == F.lit(compensation_year)
        )
        .filter(F.col("street_signature").isNotNull())
        .filter(F.trim(F.coalesce(F.col("street_signature"), F.lit(""))) != "")
        .withColumn(
            "closure_duration_sec",
            F.unix_timestamp(F.col("closure_end_ts")) - F.unix_timestamp(F.col("closure_start_ts")),
        )
    )

    businesses_filtered = silver_businesses_df.filter(
        F.col("street_signature").isNotNull()
    ).filter(
        F.trim(F.coalesce(F.col("street_signature"), F.lit(""))) != ""
    ).filter(
        F.trim(F.coalesce(F.col("street_name_raw"), F.lit(""))) != ""
    )

    id_match = (
        (F.col("s.main_street_id").isNotNull() & F.col("b.business_k_rechov").isNotNull() & (F.col("s.main_street_id") == F.col("b.business_k_rechov")))
        | ((F.col("s.street_role") == F.lit("from")) & F.col("s.start_cross_street_id").isNotNull() & F.col("b.business_k_rechov").isNotNull() & (F.col("s.start_cross_street_id") == F.col("b.business_k_rechov")))
        | ((F.col("s.street_role") == F.lit("to")) & F.col("s.end_cross_street_id").isNotNull() & F.col("b.business_k_rechov").isNotNull() & (F.col("s.end_cross_street_id") == F.col("b.business_k_rechov")))
    )
    fallback_match = (
        (F.col("s.main_street_id").isNull() | F.col("b.business_k_rechov").isNull()
         | ((F.col("s.street_role") == F.lit("from")) & F.col("s.start_cross_street_id").isNull())
         | ((F.col("s.street_role") == F.lit("to")) & F.col("s.end_cross_street_id").isNull()))
        & (F.col("b.street_signature") == F.col("s.street_signature"))
    )
    join_condition = id_match | fallback_match

    joined_df = (
        businesses_filtered.alias("b")
        .join(
            closure_2023_df.alias("s"),
            on=join_condition,
            how="inner",
        )
        .filter(F.col("s.street_signature").isNotNull())
        .filter(F.trim(F.coalesce(F.col("s.street_signature"), F.lit(""))) != "")
        .filter(~_is_municipal(F.col("b.holder_name")))
        .filter(F.col("b.street_signature").isNotNull())
        .filter(F.trim(F.coalesce(F.col("b.street_signature"), F.lit(""))) != "")
        .select(
            F.to_date(F.col("s.affected_date")).alias("date"),
            F.col("b.business_id"),
            F.coalesce(
                F.col("b.business_display_name"),
                F.col("b.holder_name"),
                F.concat(F.lit("Business #"), F.col("b.business_id").cast("string")),
            ).alias("business_name"),
            F.col("b.street_name_raw").alias("street_name"),
            F.col("b.shetach").cast("double").alias("shetach"),
            F.col("s.t_sug").alias("closure_category"),
            F.col("s.closure_duration_sec"),
            F.col("s.closure_id"),
            F.col("b.business_usage"),
            F.col("b.x_coord"),
            F.col("b.y_coord"),
        )
    )

    rank_window = Window.partitionBy("business_id", "date").orderBy(
        F.col("closure_duration_sec").desc_nulls_last(),
        F.col("closure_id").desc(),
    )
    one_per_day_df = (
        joined_df.withColumn("_rn", F.row_number().over(rank_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "closure_duration_sec")
        .dropDuplicates(["business_id", "date"])
    )

    return one_per_day_df.withColumn(
        "payout_ils",
        F.least(F.col("shetach") * F.lit(100.0), F.lit(daily_cap_ils)),
    ).withColumn(
        "is_capped",
        F.col("payout_ils") >= F.lit(daily_cap_ils),
    ).select(
        "date", "business_id", "business_name", "street_name", "payout_ils", "is_capped", "closure_category",
        "closure_id", "business_usage", "x_coord", "y_coord",
    )


def _build_gold_payout_precision_2023(
    impacted_segments_df: DataFrame,
    silver_businesses_df: DataFrame,
    silver_street_daily_df: DataFrame,
    compensation_year: int,
    daily_cap_ils: float,
) -> DataFrame:
    """Build gold_payout_precision_2023: match by segment_id only; one payout per business per day;
    when multiple closures overlap on same segment/day, keep the closure with longest duration."""
    businesses_filtered = silver_businesses_df.filter(
        F.col("street_signature").isNotNull()
    ).filter(
        F.trim(F.coalesce(F.col("street_signature"), F.lit(""))) != ""
    ).filter(
        F.trim(F.coalesce(F.col("street_name_raw"), F.lit(""))) != ""
    )

    closure_2023_df = (
        silver_street_daily_df.filter(
            F.year(F.col("affected_date")) == F.lit(compensation_year)
        )
        .groupBy("closure_id", "affected_date")
        .agg(
            F.first("t_sug").alias("t_sug"),
            F.first("closure_start_ts").alias("closure_start_ts"),
            F.first("closure_end_ts").alias("closure_end_ts"),
        )
    ).withColumn(
        "closure_duration_sec",
        F.unix_timestamp(F.col("closure_end_ts")) - F.unix_timestamp(F.col("closure_start_ts")),
    )

    impacted_with_closure_df = impacted_segments_df.join(
        closure_2023_df.select(
            "closure_id",
            "affected_date",
            "t_sug",
            "closure_duration_sec",
        ),
        on=["closure_id", "affected_date"],
        how="left",
    )

    joined_df = (
        impacted_with_closure_df.withColumn(
            "segment_id_str", F.col("segment_id").cast("string")
        )
        .alias("i")
        .join(
            businesses_filtered.withColumn(
                "assigned_segment_id_str", F.col("assigned_segment_id").cast("string")
            ).alias("b"),
            on=F.col("i.segment_id_str") == F.col("b.assigned_segment_id_str"),
            how="inner",
        )
        .filter(~_is_municipal(F.col("b.holder_name")))
        .select(
            F.to_date(F.col("i.affected_date")).alias("date"),
            F.col("b.business_id"),
            F.coalesce(
                F.col("b.business_display_name"),
                F.col("b.holder_name"),
                F.concat(F.lit("Business #"), F.col("b.business_id").cast("string")),
            ).alias("business_name"),
            F.col("b.street_name_raw").alias("street_name"),
            F.col("b.shetach").cast("double").alias("shetach"),
            F.col("i.t_sug").alias("closure_category"),
            F.col("i.closure_duration_sec"),
            F.col("i.closure_id"),
            F.col("b.business_usage"),
            F.col("b.x_coord"),
            F.col("b.y_coord"),
        )
    )

    rank_window = Window.partitionBy("business_id", "date").orderBy(
        F.col("closure_duration_sec").desc_nulls_last(),
        F.col("closure_id").desc(),
    )
    one_per_day_df = (
        joined_df.withColumn("_rn", F.row_number().over(rank_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "closure_duration_sec")
        .dropDuplicates(["business_id", "date"])
    )

    return one_per_day_df.withColumn(
        "payout_ils",
        F.least(F.col("shetach") * F.lit(100.0), F.lit(daily_cap_ils)),
    ).withColumn(
        "is_capped",
        F.col("payout_ils") >= F.lit(daily_cap_ils),
    ).select(
        "date", "business_id", "business_name", "street_name", "payout_ils", "is_capped", "closure_category",
        "closure_id", "business_usage", "x_coord", "y_coord",
    )


def _build_gold_audit_assignment_unmatched(
    silver_street_daily_df: DataFrame,
    silver_businesses_df: DataFrame,
    gold_payout_assignment_df: DataFrame,
    compensation_year: int,
) -> DataFrame:
    """One row per closure-day-street (2023) with detailed audit reasons."""
    closure_2023 = (
        silver_street_daily_df.filter(
            F.year(F.col("affected_date")) == F.lit(compensation_year)
        )
        .select(
            F.col("closure_id"),
            F.col("affected_date"),
            F.col("street_signature"),
            F.col("street_name_raw"),
            F.col("t_sug"),
        )
        .dropDuplicates(["closure_id", "affected_date", "street_signature"])
    )

    business_street_counts_df = (
        silver_businesses_df.filter(F.col("street_signature").isNotNull())
        .groupBy("street_signature")
        .agg(F.countDistinct("business_id").alias("businesses_on_street"))
    )

    closure_with_counts = (
        closure_2023.alias("c")
        .join(
            business_street_counts_df.alias("bsc"),
            F.col("c.street_signature") == F.col("bsc.street_signature"),
            "left",
        )
        .select(
            F.col("c.closure_id"),
            F.col("c.affected_date"),
            F.col("c.street_signature"),
            F.col("c.street_name_raw"),
            F.col("c.t_sug"),
            F.col("bsc.businesses_on_street").alias("businesses_on_street"),
        )
    )

    left_joined = (
        closure_with_counts.alias("c")
        .join(
            silver_businesses_df.alias("b"),
            on=(
                F.col("c.street_signature").isNotNull()
                & (F.trim(F.col("c.street_signature")) != F.lit(""))
                & (F.col("c.street_signature") == F.col("b.street_signature"))
            ),
            how="left",
        )
        .withColumn("is_municipal", _is_municipal(F.col("b.holder_name")))
    )
    agg_df = left_joined.groupBy(
        "c.closure_id",
        "c.affected_date",
        "c.street_signature",
        "c.street_name_raw",
        "c.t_sug",
        "businesses_on_street",
    ).agg(
        F.count(F.col("b.business_id")).alias("match_count"),
        F.sum(F.when(~F.col("is_municipal"), 1).otherwise(0)).alias("non_municipal_count"),
    )

    filtered = agg_df.filter(
        (F.col("c.street_signature").isNull())
        | (F.trim(F.col("c.street_signature")) == F.lit(""))
        | (F.trim(F.coalesce(F.col("c.street_name_raw"), F.lit(""))).isin("", "0"))
        | (F.col("match_count") == 0)
        | ((F.col("match_count") > 0) & (F.col("non_municipal_count") == 0))
    ).withColumn(
        "audit_reason",
        _audit_reason_column(
            street_name_col=F.col("c.street_name_raw"),
            affected_date_col=F.col("c.affected_date"),
            street_signature_col=F.col("c.street_signature"),
            businesses_on_street_col=F.col("businesses_on_street"),
            match_count_col=F.col("match_count"),
            compensation_year=compensation_year,
            include_outside_spatial=False,
        ),
    )
    filtered_result = filtered.select(
        F.col("c.closure_id").alias("closure_id"),
        F.col("c.affected_date").alias("date"),
        F.col("c.street_signature").alias("street_signature"),
        F.col("c.street_name_raw").alias("street_name"),
        F.col("c.t_sug").alias("closure_category"),
        F.col("audit_reason"),
    )

    # Final reconciliation: closure IDs from 2023 not in assignment payout (100% coverage).
    all_closure_ids_2023 = (
        silver_street_daily_df.filter(F.year(F.col("affected_date")) == F.lit(compensation_year))
        .select(F.col("closure_id"))
        .dropDuplicates()
    )
    paid_closure_ids_assignment = gold_payout_assignment_df.select(F.col("closure_id")).dropDuplicates()

    reconciliation_closure_ids = all_closure_ids_2023.join(
        paid_closure_ids_assignment, on="closure_id", how="left_anti"
    )

    reconciliation_assignment = (
        reconciliation_closure_ids.alias("r")
        .join(
            closure_2023.alias("c"),
            F.col("r.closure_id") == F.col("c.closure_id"),
            how="inner",
        )
        .select(
            F.col("c.closure_id").alias("closure_id"),
            F.col("c.affected_date").alias("date"),
            F.col("c.street_signature").alias("street_signature"),
            F.col("c.street_name_raw").alias("street_name"),
            F.col("c.t_sug").alias("closure_category"),
            F.lit("MATCHED_STREET_BUT_NO_ELIGIBLE_BUSINESSES").alias("audit_reason"),
        )
    )

    return filtered_result.unionByName(reconciliation_assignment)


def _build_gold_audit_precision_unmatched(
    impacted_segments_df: DataFrame,
    silver_businesses_df: DataFrame,
    silver_street_daily_df: DataFrame,
    gold_payout_precision_df: DataFrame,
    compensation_year: int,
) -> DataFrame:
    """One row per closure-day-segment (2023) with detailed audit reasons."""
    all_closures_2023 = (
        silver_street_daily_df.filter(
            F.year(F.col("affected_date")) == F.lit(compensation_year)
        )
        .groupBy("closure_id", "affected_date")
        .agg(
            F.first("t_sug").alias("t_sug"),
            F.first("street_name_raw").alias("street_name"),
            F.first("street_signature").alias("street_signature"),
        )
    )

    business_street_counts_df = (
        silver_businesses_df.filter(F.col("street_signature").isNotNull())
        .groupBy("street_signature")
        .agg(F.countDistinct("business_id").alias("businesses_on_street"))
    )

    # 1. Segments that mapped but failed to find valid businesses
    impacted_with_t_sug = (
        impacted_segments_df.join(
            all_closures_2023, on=["closure_id", "affected_date"], how="inner"
        )
        .join(
            business_street_counts_df.alias("bsc"),
            F.col("main_street_signature") == F.col("bsc.street_signature"),
            how="left",
        )
        .withColumn(
            "businesses_on_street",
            F.col("bsc.businesses_on_street"),
        )
    )
    left_joined = (
        impacted_with_t_sug.withColumn(
            "segment_id_str", F.col("segment_id").cast("string")
        )
        .alias("i")
        .join(
            silver_businesses_df.withColumn(
                "assigned_segment_id_str", F.col("assigned_segment_id").cast("string")
            ).alias("b"),
            on=F.col("i.segment_id_str") == F.col("b.assigned_segment_id_str"),
            how="left",
        )
        .withColumn("is_municipal", _is_municipal(F.col("b.holder_name")))
    )
    agg_df = left_joined.groupBy(
        "i.closure_id",
        "i.affected_date",
        "i.segment_id",
        "i.t_sug",
        "i.main_street_name_raw",
        "i.main_street_signature",
        "businesses_on_street",
    ).agg(
        F.count(F.col("b.business_id")).alias("match_count"),
        F.sum(F.when(~F.col("is_municipal"), 1).otherwise(0)).alias("non_municipal_count"),
    )
    filtered_segments = (
        agg_df.filter(
            (F.col("match_count") == 0)
            | ((F.col("match_count") > 0) & (F.col("non_municipal_count") == 0))
        )
        .withColumn(
            "audit_reason",
            _audit_reason_column(
                street_name_col=F.col("i.main_street_name_raw"),
                affected_date_col=F.col("i.affected_date"),
                street_signature_col=F.col("i.main_street_signature"),
                businesses_on_street_col=F.col("businesses_on_street"),
                match_count_col=F.col("match_count"),
                compensation_year=compensation_year,
                include_outside_spatial=True,
            ),
        )
        .select(
            F.col("i.closure_id").alias("closure_id"),
            F.col("i.affected_date").alias("date"),
            F.col("i.segment_id").alias("segment_id"),
            F.col("i.main_street_name_raw").alias("street_name"),
            F.col("i.main_street_signature").alias("street_signature"),
            F.col("i.t_sug").alias("closure_category"),
            F.col("audit_reason"),
        )
    )

    # 2. Closures that completely failed to map to any GIS segment (e.g. ghost streets or invalid names)
    mapped_closures = impacted_segments_df.select("closure_id", "affected_date").dropDuplicates()
    unmapped_closures = (
        all_closures_2023.join(
            mapped_closures, on=["closure_id", "affected_date"], how="left_anti"
        )
        .withColumn("segment_id", F.lit(None).cast("string"))
        .withColumn("audit_reason", F.lit("Unmapped Street (No GIS Match)"))
        .select(
            F.col("closure_id"),
            F.col("affected_date").alias("date"),
            F.col("segment_id"),
            F.col("street_name"),
            F.col("street_signature"),
            F.col("t_sug").alias("closure_category"),
            F.col("audit_reason"),
        )
    )

    # 3. Final reconciliation: closure IDs from 2023 not in precision payout (100% coverage).
    all_closure_ids_2023 = (
        silver_street_daily_df.filter(F.year(F.col("affected_date")) == F.lit(compensation_year))
        .select(F.col("closure_id"))
        .dropDuplicates()
    )
    paid_closure_ids_precision = gold_payout_precision_df.select(F.col("closure_id")).dropDuplicates()

    reconciliation_closure_ids = all_closure_ids_2023.join(
        paid_closure_ids_precision, on="closure_id", how="left_anti"
    )

    reconciliation_precision = (
        reconciliation_closure_ids.alias("r")
        .join(
            all_closures_2023.alias("a"),
            F.col("r.closure_id") == F.col("a.closure_id"),
            how="inner",
        )
        .select(
            F.col("a.closure_id"),
            F.col("a.affected_date").alias("date"),
            F.lit(None).cast("string").alias("segment_id"),
            F.col("a.street_name"),
            F.col("a.street_signature"),
            F.col("a.t_sug").alias("closure_category"),
            F.lit("MATCHED_STREET_BUT_NO_ELIGIBLE_BUSINESSES").alias("audit_reason"),
        )
    )

    return filtered_segments.unionByName(unmapped_closures).unionByName(reconciliation_precision)


def _build_gold_business_annual(
    gold_daily_df: DataFrame,
    compensation_year: int,
    daily_cap_ils: float,
) -> DataFrame:
    annual_df = (
        gold_daily_df.groupBy("business_id")
        .agg(
            F.first("business_street_name_raw", ignorenulls=True).alias("business_street_name_raw"),
            F.first("street_signature", ignorenulls=True).alias("street_signature"),
            F.first("house_number", ignorenulls=True).alias("house_number"),
            F.first("business_usage", ignorenulls=True).alias("business_usage"),
            F.first("shetach", ignorenulls=True).alias("shetach"),
            F.first("x_coord", ignorenulls=True).alias("x_coord"),
            F.first("y_coord", ignorenulls=True).alias("y_coord"),
            F.first("geometry_json", ignorenulls=True).alias("geometry_json"),
            F.countDistinct("compensation_date").alias("eligible_closed_days_2023"),
            F.sum("daily_compensation_ils").alias("annual_compensation_ils"),
        )
        .withColumn("compensation_year", F.lit(compensation_year))
        .withColumn(
            "theoretical_max_compensation_ils",
            F.col("eligible_closed_days_2023") * F.lit(daily_cap_ils),
        )
        .withColumn(
            "cost_per_area",
            F.when(F.col("shetach") > F.lit(0.0), F.col("annual_compensation_ils") / F.col("shetach")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "compensation_utilization_pct",
            F.when(
                F.col("theoretical_max_compensation_ils") > F.lit(0.0),
                (F.col("annual_compensation_ils") / F.col("theoretical_max_compensation_ils")) * F.lit(100.0),
            ).otherwise(F.lit(0.0)),
        )
    )
    return annual_df


def _build_gold_street_annual(gold_daily_df: DataFrame, compensation_year: int) -> DataFrame:
    return (
        gold_daily_df.groupBy("street_signature", "business_street_name_raw")
        .agg(
            F.countDistinct("business_id").alias("unique_businesses"),
            F.countDistinct("compensation_date").alias("closed_days_with_compensation_2023"),
            F.sum("daily_compensation_ils").alias("annual_compensation_ils"),
        )
        .withColumnRenamed("business_street_name_raw", "street_name_raw")
        .withColumn("compensation_year", F.lit(compensation_year))
    )


def _build_dashboard_metrics(
    gold_business_annual_df: DataFrame,
    gold_street_annual_df: DataFrame,
) -> DataFrame:
    return (
        gold_business_annual_df.alias("b")
        .join(
            gold_street_annual_df.select(
                "street_signature",
                "closed_days_with_compensation_2023",
                "annual_compensation_ils",
            ).withColumnRenamed("annual_compensation_ils", "street_annual_compensation_ils"),
            on="street_signature",
            how="left",
        )
        .select(
            "b.compensation_year",
            "b.business_id",
            "b.business_street_name_raw",
            "b.street_signature",
            "b.house_number",
            "b.business_usage",
            "b.shetach",
            "b.eligible_closed_days_2023",
            "b.annual_compensation_ils",
            "b.theoretical_max_compensation_ils",
            "b.cost_per_area",
            "b.compensation_utilization_pct",
            "b.x_coord",
            "b.y_coord",
            "b.geometry_json",
            "closed_days_with_compensation_2023",
            "street_annual_compensation_ils",
        )
    )


def run_gold_layer(
    spark: SparkSession,
    config: GoldConfig | None = None,
    daily_output_path: str | None = None,
    business_output_path: str | None = None,
    street_output_path: str | None = None,
    unmatched_output_path: str | None = None,
    dashboard_output_path: str | None = None,
    assignment_output_path: str | None = None,
    precision_output_path: str | None = None,
) -> dict[str, DataFrame]:
    """Run Gold transformations and write idempotent Delta outputs."""
    cfg = config or GoldConfig()
    if cfg.write_mode != "overwrite":
        raise ValueError("Gold layer supports overwrite mode only for idempotency.")

    silver_street_daily_df = spark.table(cfg.silver_street_daily_table)
    silver_businesses_df = spark.table(cfg.silver_businesses_table)
    silver_segments_df = spark.table(cfg.silver_street_segments_table)
    _validate_row_count(silver_street_daily_df, "silver_street_closures_daily")
    _validate_row_count(silver_businesses_df, "silver_businesses")
    _validate_row_count(silver_segments_df, "silver_street_segments")

    impacted_segments_df = _build_impacted_closure_segments(
        silver_street_daily_df=silver_street_daily_df,
        silver_segments_df=silver_segments_df,
        compensation_year=cfg.compensation_year,
    )

    gold_daily_df = _build_gold_daily_compensation(
        impacted_segments_df=impacted_segments_df,
        silver_businesses_df=silver_businesses_df,
        compensation_year=cfg.compensation_year,
        daily_rate_ils=cfg.daily_compensation_rate_ils,
        daily_cap_ils=cfg.daily_compensation_cap_ils,
    )
    _validate_row_count(gold_daily_df, "gold_business_daily_compensation_2023", min_rows=0)

    gold_business_annual_df = _build_gold_business_annual(
        gold_daily_df=gold_daily_df,
        compensation_year=cfg.compensation_year,
        daily_cap_ils=cfg.daily_compensation_cap_ils,
    )
    _validate_row_count(gold_business_annual_df, "gold_business_annual_compensation_2023", min_rows=0)

    gold_street_annual_df = _build_gold_street_annual(
        gold_daily_df=gold_daily_df,
        compensation_year=cfg.compensation_year,
    )
    _validate_row_count(gold_street_annual_df, "gold_street_annual_compensation_2023", min_rows=0)

    dashboard_metrics_df = _build_dashboard_metrics(
        gold_business_annual_df=gold_business_annual_df,
        gold_street_annual_df=gold_street_annual_df,
    )
    _validate_row_count(dashboard_metrics_df, "gold_dashboard_metrics_2023", min_rows=0)

    gold_payout_assignment_df = _build_gold_payout_assignment_2023(
        silver_street_daily_df=silver_street_daily_df,
        silver_businesses_df=silver_businesses_df,
        compensation_year=cfg.compensation_year,
        daily_cap_ils=cfg.daily_compensation_cap_ils,
    )
    _validate_row_count(gold_payout_assignment_df, "gold_payout_assignment_2023", min_rows=0)
    max_payout_rows = 365 * silver_businesses_df.select("business_id").distinct().count()
    _validate_payout_sanity(
        gold_payout_assignment_df,
        "gold_payout_assignment_2023",
        max_total_ils=10_000_000_000.0,
        max_rows=max_payout_rows,
    )

    gold_payout_precision_df = _build_gold_payout_precision_2023(
        impacted_segments_df=impacted_segments_df,
        silver_businesses_df=silver_businesses_df,
        silver_street_daily_df=silver_street_daily_df,
        compensation_year=cfg.compensation_year,
        daily_cap_ils=cfg.daily_compensation_cap_ils,
    )
    _validate_row_count(gold_payout_precision_df, "gold_payout_precision_2023", min_rows=0)
    _validate_payout_sanity(
        gold_payout_precision_df,
        "gold_payout_precision_2023",
        max_total_ils=10_000_000_000.0,
        max_rows=max_payout_rows,
    )

    gold_audit_assignment_unmatched_df = _build_gold_audit_assignment_unmatched(
        silver_street_daily_df=silver_street_daily_df,
        silver_businesses_df=silver_businesses_df,
        gold_payout_assignment_df=gold_payout_assignment_df,
        compensation_year=cfg.compensation_year,
    )
    _validate_row_count(
        gold_audit_assignment_unmatched_df,
        "gold_audit_assignment_unmatched",
        min_rows=0,
    )
    gold_audit_precision_unmatched_df = _build_gold_audit_precision_unmatched(
        impacted_segments_df=impacted_segments_df,
        silver_businesses_df=silver_businesses_df,
        silver_street_daily_df=silver_street_daily_df,
        gold_payout_precision_df=gold_payout_precision_df,
        compensation_year=cfg.compensation_year,
    )
    _validate_row_count(
        gold_audit_precision_unmatched_df,
        "gold_audit_precision_unmatched",
        min_rows=0,
    )

    _ensure_database(spark, cfg.gold_business_annual_table)
    _ensure_database(spark, cfg.gold_street_annual_table)
    _ensure_database(spark, cfg.gold_dashboard_metrics_table)
    _ensure_database(spark, cfg.gold_payout_assignment_table)
    _ensure_database(spark, cfg.gold_payout_precision_table)
    _ensure_database(
        spark,
        getattr(cfg, "gold_audit_assignment_unmatched_table", _DEFAULT_AUDIT_ASSIGNMENT_UNMATCHED_TABLE),
    )
    _ensure_database(
        spark,
        getattr(cfg, "gold_audit_precision_unmatched_table", _DEFAULT_AUDIT_PRECISION_UNMATCHED_TABLE),
    )

    _write_delta_table_overwrite(gold_business_annual_df, cfg.gold_business_annual_table, business_output_path)
    _write_delta_table_overwrite(gold_street_annual_df, cfg.gold_street_annual_table, street_output_path)
    _write_delta_table_overwrite(dashboard_metrics_df, cfg.gold_dashboard_metrics_table, dashboard_output_path)
    _write_delta_table_overwrite(
        gold_payout_assignment_df, cfg.gold_payout_assignment_table, assignment_output_path
    )
    _write_delta_table_overwrite(
        gold_payout_precision_df, cfg.gold_payout_precision_table, precision_output_path
    )
    _write_delta_table_overwrite(
        gold_audit_assignment_unmatched_df,
        getattr(cfg, "gold_audit_assignment_unmatched_table", _DEFAULT_AUDIT_ASSIGNMENT_UNMATCHED_TABLE),
        None,
    )
    _write_delta_table_overwrite(
        gold_audit_precision_unmatched_df,
        getattr(cfg, "gold_audit_precision_unmatched_table", _DEFAULT_AUDIT_PRECISION_UNMATCHED_TABLE),
        None,
    )
    spark.sql(
        f"OPTIMIZE {cfg.gold_payout_assignment_table} ZORDER BY (business_id, date)"
    )
    spark.sql(
        f"OPTIMIZE {cfg.gold_payout_precision_table} ZORDER BY (business_id, date)"
    )
    spark.sql(
        f"OPTIMIZE {getattr(cfg, 'gold_audit_assignment_unmatched_table', _DEFAULT_AUDIT_ASSIGNMENT_UNMATCHED_TABLE)} ZORDER BY (date)"
    )
    spark.sql(
        f"OPTIMIZE {getattr(cfg, 'gold_audit_precision_unmatched_table', _DEFAULT_AUDIT_PRECISION_UNMATCHED_TABLE)} ZORDER BY (date)"
    )

    return {
        "business_annual_gold": gold_business_annual_df,
        "street_annual_gold": gold_street_annual_df,
        "dashboard_metrics_gold": dashboard_metrics_df,
        "gold_payout_assignment_2023": gold_payout_assignment_df,
        "gold_payout_precision_2023": gold_payout_precision_df,
        "gold_audit_assignment_unmatched": gold_audit_assignment_unmatched_df,
        "gold_audit_precision_unmatched": gold_audit_precision_unmatched_df,
    }
