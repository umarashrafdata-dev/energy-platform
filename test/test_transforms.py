from datetime import date, datetime, timezone

from energy_platform.transforms import standardise_prices, RAW_SCHEMA

from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
)


def make_raw_row(**overrides) -> dict:
    """One contract-shaped raw row; override only what the test cares about."""
    row = {
        "settlementDate": "2026-09-15",
        "settlementPeriod": 10,
        "startTime": "2026-09-15T03:30:00Z",
        "createdDateTime": "2026-09-15T04:45:14Z",
        "systemSellPrice": 45.5,
        "systemBuyPrice": 45.5,
        "priceDerivationCode": "P",
        "bsadDefaulted": False,
        "netImbalanceVolume": 84.25,
        "sellPriceAdjustment": 0.0,
        "buyPriceAdjustment": 0.0,
        "replacementPrice": None,
        "replacementPriceReferenceVolume": None,
        "totalAcceptedOfferVolume": 580.44,
        "totalAcceptedBidVolume": -495.93,
        "totalAdjustmentSellVolume": 0.0,
        "totalAdjustmentBuyVolume": 0.0,
        "totalSystemTaggedAcceptedOfferVolume": 579.44,
        "totalSystemTaggedAcceptedBidVolume": -495.93,
        "totalSystemTaggedAdjustmentSellVolume": None,
        "totalSystemTaggedAdjustmentBuyVolume": None,
    }
    row.update(overrides)
    return row


def test_standardise_renames_and_casts(spark):
    raw = spark.createDataFrame([make_raw_row()], schema=RAW_SCHEMA)
    out = standardise_prices(raw)
    row = out.collect()[0]
    assert row.settlement_date == date(2026, 9, 15)
    assert row.settlement_period == 10
    assert row.start_time_utc.astimezone(timezone.utc) == datetime(2026, 9, 15, 3, 30, tzinfo=timezone.utc)
    assert float(row.system_sell_price) == 45.5
    assert "settlementDate" not in out.columns  # fully renamed


def test_rows_missing_period_are_dropped(spark):
    raw = spark.createDataFrame([
        make_raw_row(),
        make_raw_row(settlementPeriod=None),
    ], schema=RAW_SCHEMA)
    assert standardise_prices(raw).count() == 1


def test_normal_nulls_flow_through(spark):
    raw = spark.createDataFrame([make_raw_row()], schema=RAW_SCHEMA)
    row = standardise_prices(raw).collect()[0]
    assert row.replacement_price is None  # contract: null is NORMAL here