import logging
from pyspark.shell import spark
from pyspark.sql import DataFrame, SparkSession
logger = logging.getLogger(__name__)
SILVER_PRICES_TABLE = "energy.silver.system_prices"

class SilverIntegrityError(Exception):
    """Raised when the silver table is not in a valid state after a write operation."""
    pass

def read_bronze(spark: SparkSession)-> DataFrame:
    """Read the bronze system prices table."""
    return spark.table("energy.bronze.system_prices")

def overwrite_silver(df: DataFrame)-> int:
    """Overwrite the silver system prices table."""
    count = df.count()
    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_PRICES_TABLE)
    logger.info("Silver rebuilt: %d rows in %s", count, SILVER_PRICES_TABLE)
    return count

def assert_unique_keys(spark: SparkSession) -> None:
    """Assert that the silver table has unique keys (settlement_date, settlement_period)."""
    df = spark.table(SILVER_PRICES_TABLE)
    duplicates = (
        df.groupBy("settlement_date", "settlement_period")
        .count()
        .filter("count > 1")
    )
    if duplicates.count() > 0:
        raise SilverIntegrityError(
            f"Silver table {SILVER_PRICES_TABLE} has duplicate keys."
        )