from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def standardise_prices(raw_prices: DataFrame) -> DataFrame:
    """
    Standardises the prices in the raw_prices DataFrame.

    Args:
        raw_prices (DataFrame): The input DataFrame containing raw prices.
    
    Returns:
        DataFrame: A new DataFrame with standardised prices.
    
    """
    return (
        raw_prices.select(
            F.to_date("settlementDate").alias("settlement_date"),
            F.col("settlementPeriod").cast("int").alias("settlement_period"),
            F.col("systemSellPrice").cast("decimal(10,2)").alias("system_sell_price"),
            F.col("systemBuyPrice").cast("decimal(10,2)").alias("system_buy_price")
        )
        .where(F.col("settlement_date").isNotNull() & F.col("settlement_period").isNotNull())
    )