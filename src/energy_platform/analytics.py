import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

EXPECTED_PERIODS_DEFAULT = 48

def daily_price_stats(silver = DataFrame) -> DataFrame:
    daily = (
        silver.groupBy("settlement_date")
        .agg(
            F.mean("system_sell_price").alias("mean_sell_price"),
            F.min("system_sell_price").alias("min_sell_price"),
            F.max("system_sell_price").alias("max_sell_price"),
            F.stddev("system_sell_price").alias("std_sell_price"),
            F.count("settlement_period").alias("period_count"),
            F.sum("net_imbalance_volume").alias("net_niv"),
            F.sum(F.when(F.col("system_sell_price") <= 0, 1).otherwise(0)).alias("negative_period_count")
        )
    )

    w = Window.orderBy("settlement_date")
    daily = (
        daily
        .withColumn("prev_mean_price", F.lag("mean_sell_price").over(w))
        .withColumn("price_change", F.col("mean_sell_price")-F.col("prev_mean_price"))
        .withColumn(
            "log_return", 
            F.when(
                (F.col("prev_mean_price")>0) & (F.col("mean_sell_price")>0)
                ,F.log(F.col("mean_sell_price")/F.col("prev_mean_price")),).otherwise(F.lit(None)))
    )

    return (
        daily
        .withColumn("day_of_week", F.dayofweek("settlement_date"))
        .withColumn("month", F.month("settlement_date"))
        .withColumn("is_complete_day", F.col("period_count")== F.lit(EXPECTED_PERIODS_DEFAULT),)
        .drop("prev_mean_price")
    )

def rolling_volatility(daily: DataFrame) -> DataFrame:
    """One row per settlement_date: trailing 7d/30d stats over daily prices.
    Windows are ROW-based (last N observed days, not calendar days) - at this
    volume, with is_complete_day alongside, that's the honest simple choice.
    Early rows carry nulls until enough history accumulates: correct, not a bug."""
    w7 = Window.orderBy("settlement_date").rowsBetween(-6, 0)
    w30 = Window.orderBy("settlement_date").rowsBetween(-29, 0)

    out = (
        daily
        .withColumn("rolling_mean_7d", F.mean("mean_sell_price").over(w7))
        .withColumn("rolling_vol_7d", F.stddev("price_change").over(w7))
        .withColumn("rolling_mean_30d", F.mean("mean_sell_price").over(w30))
        .withColumn("rolling_vol_30d", F.stddev("price_change").over(w30))
        .withColumn(
            "mean_reversion_gap",
            F.col("mean_sell_price") - F.col("rolling_mean_30d"),
        )
        .withColumn(
            "price_zscore",
            F.when(
                F.col("rolling_vol_30d").isNotNull() & (F.col("rolling_vol_30d") > 0),
                F.col("mean_reversion_gap") / F.col("rolling_vol_30d"),
            ).otherwise(F.lit(None)),
        )
    )
    return out

def price_spikes(silver: DataFrame, vol: DataFrame, k: float = 3.0) -> DataFrame:
    """Period-grain extreme events: |price - trailing 30d mean| > k * trailing
    30d vol. Empty when the market is calm - empty is the healthy state."""
    stats = vol.select("settlement_date", "rolling_mean_30d", "rolling_vol_30d")

    return (
        silver.join(stats, on="settlement_date", how="inner")
        .withColumn(
            "spike_zscore",
            F.when(
                F.col("rolling_vol_30d").isNotNull() & (F.col("rolling_vol_30d") > 0),
                (F.col("system_sell_price") - F.col("rolling_mean_30d"))
                / F.col("rolling_vol_30d"),
            ).otherwise(F.lit(None)),
        )
        .where(F.abs(F.col("spike_zscore")) > F.lit(k))
        .select(
            "settlement_date",
            "settlement_period",
            "system_sell_price",
            "spike_zscore",
            "net_imbalance_volume",
            "price_derivation_code",
        )
    )