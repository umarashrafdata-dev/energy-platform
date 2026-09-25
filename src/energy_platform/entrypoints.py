import logging
from datetime import date

from energy_platform.logging_setup import configure_logging
from energy_platform.quality import split_on_quality, enforce_batch_threshold


logger = logging.getLogger(__name__)

def hello_job():
    """Test Entrypoint: proves wheel is alive and kicking"""
    configure_logging(logging.INFO)

    from energy_platform.settlements import settlement_period_start

    start = settlement_period_start(date(2026,10,25),50)
    logger.info("energy-platform is alive and kicking")
    logger.info("Period 50 on the long day starts at %s", start)

def ingest_prices() -> None:
    """Ingest Elexon system prices into bronze, watermark-driven."""
    configure_logging()
    from datetime import date as date_cls

    from pyspark.sql import SparkSession

    from energy_platform import bronze
    from energy_platform.elexon import fetch_system_prices
    from energy_platform.transforms import standardise_prices
    from energy_platform.watermark import dates_to_fetch, Watermark

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    wm = bronze.read_watermark(spark)
    plan = dates_to_fetch(wm, today=date_cls.today(), default_start=date_cls(2026, 9, 1))
    logger.info("Fetch plan: %d date(s)", len(plan))

    for d in plan:
            rows = fetch_system_prices(d)
            if not rows:
                logger.info("No data for %s; stopping advance here", d)
                break
            df = standardise_prices(bronze.raw_rows_to_df(spark, rows))

            good, quarantined = split_on_quality(df)
            good_count = bronze.append_prices(good)
            quarantine_count = bronze.append_quarantine(quarantined)
            logger.info(
                "%s: %d good, %d quarantined", d, good_count, quarantine_count
            )
            enforce_batch_threshold(good_count, quarantine_count)

            last = max(rows, key=lambda r: r["settlementPeriod"])
            bronze.write_watermark(spark, Watermark(d, last["settlementPeriod"]))

def build_silver() -> None:
    """Rebuild silver from bronze: dedupe to latest version per key."""
    configure_logging()
    from pyspark.sql import SparkSession

    from alternatives import silver
    from energy_platform.transforms import dedupe_latest

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    deduped = dedupe_latest(silver.read_bronze(spark))
    silver.overwrite_silver(deduped)
    silver.assert_unique_keys(spark)