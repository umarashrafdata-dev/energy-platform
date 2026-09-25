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