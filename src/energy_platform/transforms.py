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
            F.to_timestamp("startTime").alias("start_time_utc"),
            F.to_timestamp("createdDateTime").alias("created_datetime_utc"),
            F.col("systemSellPrice").cast("decimal(10,2)").alias("system_sell_price"),
            F.col("systemBuyPrice").cast("decimal(10,2)").alias("system_buy_price"),
            F.col("priceDerivationCode").alias("price_derivation_code"),
            F.col("bsadDefaulted").cast("boolean").alias("bsad_defaulted"),
            F.col("netImbalanceVolume").cast("decimal(18,6)").alias("net_imbalance_volume"),
            F.col("sellPriceAdjustment").cast("decimal(10,2)").alias("sell_price_adjustment"),
            F.col("buyPriceAdjustment").cast("decimal(10,2)").alias("buy_price_adjustment"),
            F.col("replacementPrice").cast("decimal(10,2)").alias("replacement_price"),
            F.col("replacementPriceReferenceVolume").cast("decimal(18,6)").alias("replacement_price_reference_volume"),
            F.col("totalAcceptedOfferVolume").cast("decimal(18,6)").alias("total_accepted_offer_volume"),
            F.col("totalAcceptedBidVolume").cast("decimal(18,6)").alias("total_accepted_bid_volume"),
            F.col("totalAdjustmentSellVolume").cast("decimal(18,6)").alias("total_adjustment_sell_volume"),
            F.col("totalAdjustmentBuyVolume").cast("decimal(18,6)").alias("total_adjustment_buy_volume"),
            F.col("totalSystemTaggedAcceptedOfferVolume").cast("decimal(18,6)").alias("total_system_tagged_accepted_offer_volume"),
            F.col("totalSystemTaggedAcceptedBidVolume").cast("decimal(18,6)").alias("total_system_tagged_accepted_bid_volume"),
            F.col("totalSystemTaggedAdjustmentSellVolume").cast("decimal(18,6)").alias("total_system_tagged_adjustment_sell_volume"),
            F.col("totalSystemTaggedAdjustmentBuyVolume").cast("decimal(18,6)").alias("total_system_tagged_adjustment_buy_volume"),
        )
        .where(F.col("settlement_date").isNotNull() & F.col("settlement_period").isNotNull())
    )