"""Silver-layer transformations for street closures, segments, and businesses."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType

from src.common.utils import (
    euclidean_distance_m,
    generate_street_signature,
    parse_paths_array_json,
    point_to_polyline_distance_m,
    segment_midpoint_from_paths_json,
)
from .config import SilverConfig

INELIGIBLE_USAGE_TYPES = [
    "חדר טרנספורמציה",
    "חניונים במבנה ללא תשלום",
    "בתי כנסת",
    "מחלקות העירייה",
    "בתי ספר בפיקוח",
    "גני ילדים עירוניים",
    "מגרש חניה ללא תשלום",
    "דירה במלון",
    "בתי תפילה",
    "שגרירויות וקונסוליות",
    "בתי תפילה נוצרים",
    "מקלט",
    "מוסדות ציבור",
    "חברות עירוניות",
    "בתי עלמין",
    "מתקן לטהור מים",
    "שנאים תחנת משנה",
    "שטחים טכניים",
    "יחידה סגורה",
    "תנועות נוער",
    "שטח קרקע שעיקר שימושו עם המבנה",
    "חצר המשמשת גני ילדים",
]


def _build_usage_type_eligibility_df(spark: SparkSession) -> DataFrame:
    """Return DataFrame of ineligible usage types. All others are eligible."""
    from pyspark.sql import Row

    rows = [Row(usage_type=t) for t in INELIGIBLE_USAGE_TYPES]
    return spark.createDataFrame(rows)


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


def create_street_signature(street_col: Column) -> Column:
    """Backward-compatible wrapper around shared signature generator."""
    return generate_street_signature(street_col)


def _safe_string_col(col_name: str) -> Column:
    """Return trimmed string column with empty values normalized to NULL."""
    trimmed = F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
    return F.when(trimmed == F.lit(""), F.lit(None)).otherwise(trimmed)


def _safe_house_number_int(col_name: str) -> Column:
    """Extract first numeric token from house number text."""
    return F.regexp_extract(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"(\d+)", 1).cast("int")


def _safe_cast_long(col: Column) -> Column:
    """Cast to long, returning NULL for malformed input (e.g. '507-12563') instead of raising."""
    return col.try_cast("long")


def build_silver_street_closures_daily(
    spark: SparkSession,
    bronze_street_table: str,
) -> DataFrame:
    """Normalize closures and produce one record per closure-street-day."""
    bronze_df = spark.table(bronze_street_table)
    _validate_row_count(bronze_df, "bronze_street_closures")

    datetime_fmt = "dd/MM/yyyy H:mm"
    parsed_df = (
        bronze_df.withColumn("closure_id", _safe_cast_long(F.col("ID")))
        .withColumn("main_street_id", _safe_cast_long(F.col("id_rechov")))
        .withColumn("start_cross_street_id", _safe_cast_long(F.col("me_k_rechov")))
        .withColumn("end_cross_street_id", _safe_cast_long(F.col("ad_k_rechov")))
        .withColumn("main_street_name_raw", F.col("me_shem_rechov").cast("string"))
        .withColumn("tr_from_clean", _safe_string_col("tr_from"))
        .withColumn("tr_to_clean", _safe_string_col("tr_to"))
        .withColumn("closure_start_ts", F.to_timestamp("tr_from_clean", datetime_fmt))
        .withColumn("closure_end_ts_raw", F.to_timestamp("tr_to_clean", datetime_fmt))
        .withColumn("closure_end_ts", F.coalesce(F.col("closure_end_ts_raw"), F.col("closure_start_ts")))
        .withColumn("closure_start_date", F.to_date("closure_start_ts"))
        .withColumn("closure_end_date", F.to_date("closure_end_ts"))
        .withColumn(
            "safe_start_date",
            F.least(F.col("closure_start_date"), F.col("closure_end_date")),
        )
        .withColumn(
            "safe_end_date",
            F.greatest(F.col("closure_start_date"), F.col("closure_end_date")),
        )
        .filter(F.col("safe_start_date").isNotNull())
        .filter(F.col("safe_end_date").isNotNull())
        .withColumn("affected_date", F.explode(F.sequence(F.col("safe_start_date"), F.col("safe_end_date"))))
        .withColumn(
            "street_entries",
            F.array(
                F.struct(
                    F.lit("from").alias("street_role"),
                    F.col("me_shem_rechov").cast("string").alias("street_name_raw"),
                ),
                F.struct(
                    F.lit("to").alias("street_role"),
                    F.col("ad_shem_rechov").cast("string").alias("street_name_raw"),
                ),
            ),
        )
        .withColumn("street_entry", F.explode("street_entries"))
        .select(
            "closure_id",
            "affected_date",
            "closure_start_ts",
            "closure_end_ts",
            "tr_from",
            "tr_to",
            "k_sug",
            "t_sug",
            "k_sgira",
            "shaot",
            "sw_laila",
            "main_street_id",
            "start_cross_street_id",
            "end_cross_street_id",
            "main_street_name_raw",
            F.col("street_entry.street_role").alias("street_role"),
            F.col("street_entry.street_name_raw").alias("street_name_raw"),
        )
        .filter(F.trim(F.coalesce(F.col("street_name_raw"), F.lit(""))) != "")
        .withColumn("main_street_signature", create_street_signature(F.col("main_street_name_raw")))
        .withColumn("street_signature", create_street_signature(F.col("street_name_raw")))
        .filter(F.col("street_signature").isNotNull())
        .filter(F.col("street_signature") != "")
        .filter(F.col("closure_id").isNotNull())
    )

    # Protect against duplicate unpivot rows when both sides resolve to the same street.
    silver_street_daily_df = parsed_df.dropDuplicates(
        ["closure_id", "affected_date", "street_signature"]
    )
    _validate_row_count(silver_street_daily_df, "silver_street_closures_daily")
    return silver_street_daily_df


def build_silver_street_segments(
    spark: SparkSession,
    bronze_street_segments_table: str,
) -> DataFrame:
    """Normalize street segments from Layer 507 and derive segment geometry metadata.
    Geometry is parsed via UDFs so coordinates are correctly extracted from paths JSON."""
    bronze_segments_df = spark.table(bronze_street_segments_table)
    _validate_row_count(bronze_segments_df, "bronze_street_segments")

    # Use paths_json (coalesce to "[]" so UDF always receives a string)
    paths_json_col = F.coalesce(F.col("paths_json"), F.lit("[]"))

    silver_segments_df = (
        bronze_segments_df.withColumn("k_rechov", F.coalesce(F.col("k_rechov").cast("string"), F.lit(None)))
        .withColumn("oid_rechov", F.col("oid_rechov").cast("string"))
        .withColumn("left_from", F.col("left_from").try_cast("int"))
        .withColumn("left_to", F.col("left_to").try_cast("int"))
        .withColumn("right_from", F.col("right_from").try_cast("int"))
        .withColumn("right_to", F.col("right_to").try_cast("int"))
        .withColumn("ms_merechov", F.col("ms_merechov").cast("string"))
        .withColumn("ms_lerechov", F.col("ms_lerechov").cast("string"))
        .withColumn("shape_length_m", F.col("shape_length").cast("double"))
        .withColumn(
            "street_name_raw",
            F.coalesce(F.col("t_shem_rechov").cast("string"), F.col("t_rechov").cast("string")),
        )
        .withColumn("street_signature", create_street_signature(F.col("street_name_raw")))
        .withColumn("paths_array", parse_paths_array_json(paths_json_col))
        .withColumn("mid_struct", segment_midpoint_from_paths_json(paths_json_col))
        .withColumn("segment_mid_x", F.col("mid_struct.mid_x"))
        .withColumn("segment_mid_y", F.col("mid_struct.mid_y"))
        .drop("mid_struct")
        .withColumn(
            "segment_id",
            F.coalesce(
                F.col("unique_id"),
                F.concat_ws("_", F.col("k_rechov").cast("string"), F.col("feature_index").cast("string")),
            ),
        )
        .filter(F.trim(F.coalesce(F.col("street_name_raw"), F.lit(""))) != "")
        .filter(F.col("street_signature").isNotNull())
        .filter(F.col("street_signature") != "")
    )

    _validate_row_count(silver_segments_df, "silver_street_segments")
    return silver_segments_df


def _build_silver_businesses_base(
    spark: SparkSession,
    bronze_business_features_table: str,
) -> DataFrame:
    bronze_df = spark.table(bronze_business_features_table)
    _validate_row_count(bronze_df, "bronze_businesses_features")

    attributes = "attributes_json"
    geometry = "geometry_json"

    staged_df = (
        bronze_df.select(
            _safe_cast_long(F.col("feature_index")).alias("feature_index"),
            F.col("page_number").cast("int").alias("page_number"),
            F.col("result_offset").cast("int").alias("result_offset"),
            F.col("bronze_source_url"),
            F.col("bronze_ingested_at_utc"),
            F.col("attributes_json"),
            F.col("geometry_json"),
            F.get_json_object(attributes, "$.id_esek").cast("string").alias("business_id"),
            F.get_json_object(attributes, "$.OBJECTID").cast("string").alias("object_id"),
            _safe_cast_long(F.get_json_object(attributes, "$.k_rechov")).alias("business_k_rechov"),
            F.get_json_object(attributes, "$.shimush").cast("string").alias("business_usage"),
            F.get_json_object(attributes, "$.shem_rechov").cast("string").alias("street_name_raw"),
            F.get_json_object(attributes, "$.ms_bayit").cast("string").alias("house_number"),
            F.get_json_object(attributes, "$.sw_taun_rishui").cast("int").alias("requires_license"),
            F.get_json_object(attributes, "$.shem_machzik_rashi").cast("string").alias("holder_name"),
            F.get_json_object(attributes, "$.ms_mezahe_machzik_rashi")
            .cast("string")
            .alias("holder_identifier"),
            F.get_json_object(attributes, "$.date_import").cast("string").alias("date_import_raw"),
            _safe_cast_long(F.get_json_object(attributes, "$.tr_hakama")).alias("established_epoch_ms"),
            F.get_json_object(attributes, "$.x_coord").cast("double").alias("x_coord_attr"),
            F.get_json_object(attributes, "$.y_coord").cast("double").alias("y_coord_attr"),
            F.get_json_object(geometry, "$.x").cast("double").alias("x_coord_geom"),
            F.get_json_object(geometry, "$.y").cast("double").alias("y_coord_geom"),
            F.get_json_object(attributes, "$.shetach").cast("string").alias("shetach_raw"),
        )
        .withColumn("street_signature", create_street_signature(F.col("street_name_raw")))
        .withColumn("date_import_clean", _safe_string_col("date_import_raw"))
        .withColumn("date_import_ts", F.to_timestamp("date_import_clean", "dd/MM/yyyy HH:mm:ss"))
        .withColumn(
            "established_ts",
            F.to_timestamp((F.col("established_epoch_ms") / F.lit(1000)).cast("double")),
        )
        .withColumn("x_coord", F.coalesce(F.col("x_coord_geom"), F.col("x_coord_attr")))
        .withColumn("y_coord", F.coalesce(F.col("y_coord_geom"), F.col("y_coord_attr")))
        .withColumn("house_number_int", _safe_house_number_int("house_number"))
        .withColumn(
            "shetach_clean_text",
            F.regexp_replace(F.coalesce(F.col("shetach_raw"), F.lit("")), r"[^0-9.\-]", ""),
        )
        .withColumn("shetach_double_raw", F.col("shetach_clean_text").cast("double"))
        .withColumn(
            "shetach",
            F.when(
                F.col("shetach_double_raw").isNull() | (F.col("shetach_double_raw") <= F.lit(0.0)),
                F.lit(0.0),
            ).otherwise(F.col("shetach_double_raw")),
        )
        .filter(F.col("business_id").isNotNull())
        .filter(F.trim(F.coalesce(F.col("street_name_raw"), F.lit(""))) != "")
        .filter(F.col("street_signature").isNotNull())
        .filter(F.col("street_signature") != "")
    )

    usage_type_eligibility_df = _build_usage_type_eligibility_df(spark)
    eligible_staged_df = staged_df.join(
        usage_type_eligibility_df,
        staged_df.business_usage == usage_type_eligibility_df.usage_type,
        "left_anti",
    ).withColumn(
        "business_display_name",
        F.when(
            F.col("holder_name").isNotNull()
            & (F.trim(F.coalesce(F.col("holder_name"), F.lit(""))) != ""),
            F.col("holder_name"),
        ).otherwise(
            F.concat(
                F.col("business_usage"),
                F.lit(" - "),
                F.col("street_name_raw"),
                F.lit(" "),
                F.coalesce(F.col("house_number"), F.lit("")),
                F.lit(" (ID: "),
                F.col("business_id").cast("string"),
                F.lit(")"),
            ),
        ),
    )

    dedup_window = Window.partitionBy("business_id").orderBy(
        F.col("date_import_ts").desc_nulls_last(),
        F.col("object_id").desc_nulls_last(),
        F.col("feature_index").desc_nulls_last(),
    )
    return (
        eligible_staged_df.withColumn("row_num", F.row_number().over(dedup_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num", "shetach_clean_text", "shetach_double_raw", "date_import_clean")
    )


def build_silver_businesses(
    spark: SparkSession,
    bronze_business_features_table: str,
    silver_segments_df: DataFrame,
    max_segment_distance_m: float,
    max_segment_fallback_distance_m: float,
    long_segment_threshold_m: float,
) -> DataFrame:
    """Flatten businesses and enrich each business with a best-matching street segment."""
    base_df = _build_silver_businesses_base(spark=spark, bronze_business_features_table=bronze_business_features_table)

    segments_filtered = silver_segments_df.select(
        "segment_id",
        "k_rechov",
        F.col("street_signature").alias("segment_street_signature"),
        "paths_array",
        "shape_length_m",
        "left_from",
        "left_to",
        "right_from",
        "right_to",
        "segment_mid_x",
        "segment_mid_y",
    )
    b = base_df.alias("b")
    s = F.broadcast(segments_filtered.alias("s"))

    # Two parallel Hash Joins (avoids Nested Loop Join); row_number() handles deduplication
    join_on_k_rechov = F.col("b.business_k_rechov").cast("string") == F.col("s.k_rechov")
    join_on_street_sig = (
        F.col("b.street_signature").isNotNull()
        & (F.trim(F.coalesce(F.col("b.street_signature"), F.lit(""))) != "")
        & (F.col("b.street_signature") != "0")
        & (F.col("b.street_signature") == F.col("s.segment_street_signature"))
    )

    candidates_a = (
        b.join(s, on=join_on_k_rechov, how="left")
        .withColumn(
            "segment_distance_polyline_m",
            point_to_polyline_distance_m(F.col("b.x_coord"), F.col("b.y_coord"), F.col("s.paths_array")),
        )
        .withColumn(
            "segment_distance_midpoint_m",
            euclidean_distance_m(
                F.col("b.x_coord"),
                F.col("b.y_coord"),
                F.col("s.segment_mid_x"),
                F.col("s.segment_mid_y"),
            ),
        )
        .withColumn(
            "segment_distance_m",
            F.coalesce(F.col("segment_distance_polyline_m"), F.col("segment_distance_midpoint_m")),
        )
        .withColumn(
            "house_range_match",
            (
                (
                    F.col("b.house_number_int").between(F.col("s.left_from"), F.col("s.left_to"))
                    & F.col("s.left_from").isNotNull()
                    & F.col("s.left_to").isNotNull()
                )
                | (
                    F.col("b.house_number_int").between(F.col("s.right_from"), F.col("s.right_to"))
                    & F.col("s.right_from").isNotNull()
                    & F.col("s.right_to").isNotNull()
                )
            ),
        )
        .withColumn("segment_is_long", F.coalesce(F.col("s.shape_length_m"), F.lit(0.0)) >= F.lit(long_segment_threshold_m))
        .withColumn(
            "segment_priority",
            F.when(F.col("s.segment_id").isNull(), F.lit(99))
            .when(F.col("segment_is_long") & F.col("house_range_match"), F.lit(0))
            .when(F.col("house_range_match"), F.lit(1))
            .when(F.col("segment_distance_m") <= F.lit(max_segment_distance_m), F.lit(2))
            .when(F.col("segment_distance_m") <= F.lit(max_segment_fallback_distance_m), F.lit(3))
            .otherwise(F.lit(4)),
        )
    )
    candidates_b = (
        b.join(s, on=join_on_street_sig, how="left")
        .withColumn(
            "segment_distance_polyline_m",
            point_to_polyline_distance_m(F.col("b.x_coord"), F.col("b.y_coord"), F.col("s.paths_array")),
        )
        .withColumn(
            "segment_distance_midpoint_m",
            euclidean_distance_m(
                F.col("b.x_coord"),
                F.col("b.y_coord"),
                F.col("s.segment_mid_x"),
                F.col("s.segment_mid_y"),
            ),
        )
        .withColumn(
            "segment_distance_m",
            F.coalesce(F.col("segment_distance_polyline_m"), F.col("segment_distance_midpoint_m")),
        )
        .withColumn(
            "house_range_match",
            (
                (
                    F.col("b.house_number_int").between(F.col("s.left_from"), F.col("s.left_to"))
                    & F.col("s.left_from").isNotNull()
                    & F.col("s.left_to").isNotNull()
                )
                | (
                    F.col("b.house_number_int").between(F.col("s.right_from"), F.col("s.right_to"))
                    & F.col("s.right_from").isNotNull()
                    & F.col("s.right_to").isNotNull()
                )
            ),
        )
        .withColumn("segment_is_long", F.coalesce(F.col("s.shape_length_m"), F.lit(0.0)) >= F.lit(long_segment_threshold_m))
        .withColumn(
            "segment_priority",
            F.when(F.col("s.segment_id").isNull(), F.lit(99))
            .when(F.col("segment_is_long") & F.col("house_range_match"), F.lit(0))
            .when(F.col("house_range_match"), F.lit(1))
            .when(F.col("segment_distance_m") <= F.lit(max_segment_distance_m), F.lit(2))
            .when(F.col("segment_distance_m") <= F.lit(max_segment_fallback_distance_m), F.lit(3))
            .otherwise(F.lit(4)),
        )
    )
    candidates_df = candidates_a.unionByName(candidates_b, allowMissingColumns=True)

    rank_window = Window.partitionBy(F.col("b.business_id")).orderBy(
        F.col("segment_priority").asc(),
        F.col("segment_distance_m").asc_nulls_last(),
    )

    best_segment_df = (
        candidates_df.withColumn("row_num", F.row_number().over(rank_window))
        .filter(F.col("row_num") == 1)
        .select(
            F.col("b.business_id").alias("business_id"),
            F.when(F.col("segment_priority") <= F.lit(3), F.col("s.segment_id")).alias("assigned_segment_id"),
            F.when(F.col("segment_priority") <= F.lit(3), F.col("s.k_rechov")).alias("assigned_segment_k_rechov"),
            F.when(F.col("segment_priority") <= F.lit(3), F.col("s.segment_street_signature")).alias("assigned_segment_street_signature"),
            F.when(F.col("segment_priority") <= F.lit(3), F.col("segment_distance_m")).alias("segment_distance_m"),
            F.when(
                F.col("segment_priority") <= F.lit(1),
                F.lit("HOUSE_RANGE"),
            )
            .when(F.col("segment_priority") == F.lit(2), F.lit("NEAREST_POLYLINE"))
            .when(F.col("segment_priority") == F.lit(3), F.lit("NEAREST_FALLBACK_CAPPED_1000M"))
            .otherwise(F.lit("NO_SEGMENT_MATCH"))
            .alias("segment_match_method"),
        )
    )

    silver_businesses_df = (
        base_df.join(best_segment_df, on="business_id", how="left")
        .withColumn(
            "spatial_match_available",
            F.when(F.col("assigned_segment_id").isNotNull(), F.lit(True)).otherwise(F.lit(False)),
        )
    )

    _validate_row_count(silver_businesses_df, "silver_businesses")
    return silver_businesses_df


def run_silver_layer(
    spark: SparkSession,
    config: SilverConfig | None = None,
    street_output_path: str | None = None,
    businesses_output_path: str | None = None,
    segments_output_path: str | None = None,
) -> dict[str, DataFrame]:
    """Run Silver transformations and write idempotent Delta outputs."""
    cfg = config or SilverConfig()
    if cfg.write_mode != "overwrite":
        raise ValueError("Silver layer supports overwrite mode only for idempotency.")

    _ensure_database(spark, cfg.silver_street_daily_table)
    _ensure_database(spark, cfg.silver_businesses_table)
    _ensure_database(spark, cfg.silver_street_segments_table)

    silver_street_daily_df = build_silver_street_closures_daily(
        spark=spark,
        bronze_street_table=cfg.bronze_street_table,
    )
    silver_segments_df = build_silver_street_segments(
        spark=spark,
        bronze_street_segments_table=cfg.bronze_street_segments_table,
    )
    silver_businesses_df = build_silver_businesses(
        spark=spark,
        bronze_business_features_table=cfg.bronze_business_features_table,
        silver_segments_df=silver_segments_df,
        max_segment_distance_m=cfg.max_segment_distance_m,
        max_segment_fallback_distance_m=cfg.max_segment_fallback_distance_m,
        long_segment_threshold_m=cfg.long_segment_threshold_m,
    )

    _write_delta_table_overwrite(
        df=silver_street_daily_df,
        table_name=cfg.silver_street_daily_table,
        table_path=street_output_path,
    )
    _write_delta_table_overwrite(
        df=silver_businesses_df,
        table_name=cfg.silver_businesses_table,
        table_path=businesses_output_path,
    )
    _write_delta_table_overwrite(
        df=silver_segments_df,
        table_name=cfg.silver_street_segments_table,
        table_path=segments_output_path,
    )
    spark.sql(f"OPTIMIZE {cfg.silver_businesses_table} ZORDER BY (business_id)")
    for table in [cfg.silver_street_daily_table, cfg.silver_businesses_table, cfg.silver_street_segments_table]:
        spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")

    return {
        "street_closures_silver": silver_street_daily_df,
        "street_segments_silver": silver_segments_df,
        "businesses_silver": silver_businesses_df,
    }
