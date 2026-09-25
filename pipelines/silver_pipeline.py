import dlt    
from energy_platform.transforms import dedupe_latest
from energy_platform import analytics

@dlt.table(
    name="system_prices",
    comment="Silver table of Elexon system prices, deduped to latest version per key"
)
def system_prices():
    """Silver table of Elexon system prices, deduped to latest version per key"""
    return dedupe_latest(spark.table("energy.bronze.system_prices"))

@dlt.table(
    name="silver_duplicates_keys",
    comment = "Integrity monitor, must be empty"
)
@dlt.expect_or_fail("no_duplicate_keys","key_count=1")
def silver_duplicate_keys():
    return (
        dlt.read("system_prices")
        .groupby("settlement_date", "settlement_period")
        .count()
        .withColumnRenamed("count","key_count")
    )

@dlt.table(
    name="energy.gold.daily_price_stats",
    comment="One row per settlement date: price shape, imbalance, model-prep columns.",
)
def gold_daily_price_stats():
    return analytics.daily_price_stats(dlt.read("system_prices"))


@dlt.table(
    name="energy.gold.rolling_volatility",
    comment="Trailing 7d/30d realised vol, z-score, mean-reversion gap.",
)
def gold_rolling_volatility():
    return analytics.rolling_volatility(dlt.read("energy.gold.daily_price_stats"))


@dlt.table(
    name="energy.gold.price_spikes",
    comment="Period-grain extreme cash-out events (|z| > 3). Empty when calm.",
)
def gold_price_spikes():
    return analytics.price_spikes(
        dlt.read("system_prices"),
        dlt.read("energy.gold.rolling_volatility"),
    )