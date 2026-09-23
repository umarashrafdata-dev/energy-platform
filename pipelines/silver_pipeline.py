import dlt    
from energy_platform.transforms import dedupe_latest

@dlt.table(
    name="system_prices",
    comment="Silver table of Elexon system prices, deduped to latest version per key"
)
def system_prices():
    """Silver table of Elexon system prices, deduped to latest version per key"""
    return dedupe_latest(spark.read("energy.bronze.system_prices"))

@dlt.table(
    name="silver_duplicates_keys",
    comment = "Integrity monitor, must be empty"
)
@dlt.expect_or_fail("ni_duplicate_keys","key_count=1")
def silver_duplicate_keys():
    return (
        dlt.read("system_prices")
        .groupby("settlement_date", "settlement_period")
        .count()
        .withColumnRenamed("count","key_count")
    )