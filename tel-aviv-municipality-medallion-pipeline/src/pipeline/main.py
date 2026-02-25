"""End-to-end medallion pipeline runner."""

from __future__ import annotations

import sys
sys.dont_write_bytecode = True

from pyspark.sql import SparkSession

try:
    from .bronze import run_bronze_layer
    from .config import BronzeConfig, GoldConfig, SilverConfig, SnowflakeConfig, snowflake_config_from_env
    from .gold import export_to_snowflake, run_gold_layer
    from .silver import run_silver_layer
except ImportError:
    # Allows running as a plain Python file in Databricks jobs.
    from src.pipeline.bronze import run_bronze_layer
    from src.pipeline.config import (
        BronzeConfig,
        GoldConfig,
        SilverConfig,
        SnowflakeConfig,
        snowflake_config_from_env,
    )
    from src.pipeline.gold import export_to_snowflake, run_gold_layer
    from src.pipeline.silver import run_silver_layer


def _get_or_create_spark() -> SparkSession:
    """Create Spark session for local/Databricks execution."""
    return SparkSession.builder.appName("tel-aviv-medallion-pipeline").getOrCreate()


def run_pipeline(
    spark: SparkSession | None = None,
    bronze_config: BronzeConfig | None = None,
    silver_config: SilverConfig | None = None,
    gold_config: GoldConfig | None = None,
    snowflake_config: SnowflakeConfig | None = None,
) -> dict[str, dict]:
    """Run Bronze -> Silver -> Gold and return DataFrame handles by layer.
    Optionally export all gold tables to Snowflake when snowflake_config
    is provided with valid credentials. Table and column names are converted
    to uppercase for Snowflake convention. Skips export when credentials are
    missing (no-op for local tests).
    """
    spark_session = spark or _get_or_create_spark()

    bronze_outputs = run_bronze_layer(spark=spark_session, config=bronze_config)
    silver_outputs = run_silver_layer(spark=spark_session, config=silver_config)
    gold_outputs = run_gold_layer(spark=spark_session, config=gold_config)

    # Export to Snowflake when credentials are available
    sf_cfg = snowflake_config or snowflake_config_from_env()
    if not sf_cfg or not sf_cfg.user or not sf_cfg.password:
        print("Snowflake export SKIPPED: SNOWFLAKE_USER and SNOWFLAKE_PASSWORD not set")
    else:
        for table_name, df in gold_outputs.items():
            try:
                # Convert all column names to uppercase
                upper_cols = [c.upper() for c in df.columns]
                df_upper = df.toDF(*upper_cols)

                # Log columns for payout tables (helps verify new columns are present)
                if "payout" in table_name.lower():
                    print(f"Exporting {table_name.upper()} columns: {upper_cols}")

                # Export to Snowflake with uppercase table name
                export_to_snowflake(
                    df_upper,
                    table_name.upper(),
                    sf_cfg,
                )
                print(f"Snowflake export OK: {table_name.upper()}")
            except Exception as e:
                print(f"Snowflake export FAILED for {table_name}: {e}")

    return {
        "bronze": bronze_outputs,
        "silver": silver_outputs,
        "gold": gold_outputs,
    }


if __name__ == "__main__":
    outputs = run_pipeline()
    # Simple run summary for quick sanity checks.
    for layer_name, layer_outputs in outputs.items():
        for output_name, df in layer_outputs.items():
            print(f"{layer_name}.{output_name}: {df.count()} rows")
