from datetime import date

from energy_platform.transforms import standardise_prices

def test_standardise_renames_and_casts(spark):
    # Sample input DataFrame
    raw_data = spark.createDataFrame(
        [("2026-09-16", "10", "45.50", "45.50")],
        ["settlementDate", "settlementPeriod", "systemSellPrice", "systemBuyPrice"],
    )
    out = standardise_prices(raw_data)
    row = out.collect()[0]
    assert row["settlement_date"] == date(2026, 9, 16)
    assert row["settlement_period"] == 10
    assert out.columns == ["settlement_date", "settlement_period", "system_sell_price", "system_buy_price"]


def test_rows_missing_periods_are_dropped(spark):
    # Sample input DataFrame with a row missing settlementPeriod
    raw_data = spark.createDataFrame(
        [("2026-09-16", None, "45.50", "45.50"),
         ("2026-09-16", "10", "45.50", "45.50")],
        ["settlementDate", "settlementPeriod", "systemSellPrice", "systemBuyPrice"],
    )
    out = standardise_prices(raw_data)
    assert out.count() == 1