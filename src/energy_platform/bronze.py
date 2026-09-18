"""Bronze-layer I/O: Delta writes and watermark persistence. Thin by design."""
import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession

from energy_platform.watermark import Watermark

logger = logging.getLogger(__name__)

PRICES_TABLE = "energy.bronze.system_prices"
WATERMARK_TABLE = "energy.bronze._ingest_watermark"


def append_prices(df: DataFrame) -> int:
    """Append standardised price rows to bronze. Returns row count written."""
    count = df.count()
    df.write.mode("append").saveAsTable(PRICES_TABLE)
    logger.info("Appended %d rows to %s", count, PRICES_TABLE)
    return count


def read_watermark(spark: SparkSession, source: str = "elexon_system_prices") -> Watermark | None:
    """Latest watermark for a source, or None if never ingested."""
    if not spark.catalog.tableExists(WATERMARK_TABLE):
        return None
    rows = (
        spark.table(WATERMARK_TABLE)
        .where(f"source = '{source}'")
        .orderBy("updated_at", ascending=False)
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return Watermark(rows[0].settlement_date, rows[0].settlement_period)


def write_watermark(spark: SparkSession, wm: Watermark, source: str = "elexon_system_prices") -> None:
    """Record ingestion progress (append-only history of positions)."""
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [(source, wm.settlement_date, wm.settlement_period)],
        ["source", "settlement_date", "settlement_period"],
    ).withColumn("updated_at", F.current_timestamp())
    df.write.mode("append").saveAsTable(WATERMARK_TABLE)
    logger.info("Watermark advanced: %s -> %s p%d", source, wm.settlement_date, wm.settlement_period)

def raw_rows_to_df(spark: SparkSession, rows: list[dict]) -> DataFrame:
    """Cross the Python->Spark boundary: raw API dicts to a typed DataFrame."""
    from energy_platform.transforms import RAW_SCHEMA
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)