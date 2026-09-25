from energy_platform.analytics import daily_price_stats
from energy_platform.transforms import RAW_SCHEMA, dedupe_latest, standardise_prices
from test_transforms import make_raw_row


def _gold_input(spark, rows):
    return dedupe_latest(standardise_prices(spark.createDataFrame(rows, schema=RAW_SCHEMA)))


def test_daily_stats_aggregates_one_day(spark):
    rows = [
        make_raw_row(settlementPeriod=1, systemSellPrice=40.0),
        make_raw_row(settlementPeriod=2, systemSellPrice=60.0),
    ]
    out = daily_price_stats(_gold_input(spark, rows)).collect()
    assert len(out) == 1
    assert float(out[0].mean_sell_price) == 50.0



def test_price_change_between_days(spark):
    rows = [
        make_raw_row(settlementDate="2026-09-15", settlementPeriod=1, systemSellPrice=50.0),
        make_raw_row(settlementDate="2026-09-16", settlementPeriod=1, systemSellPrice=55.0),
    ]
    out = daily_price_stats(_gold_input(spark, rows)).orderBy("settlement_date").collect()
    assert out[0].price_change is None          
    assert float(out[1].price_change) == 5.0


def test_log_return_guard_blocks_nonpositive_prices(spark):
    rows = [
        make_raw_row(settlementDate="2026-09-15", settlementPeriod=1, systemSellPrice=10.0),
        make_raw_row(settlementDate="2026-09-16", settlementPeriod=1, systemSellPrice=-5.0),
    ]
    out = daily_price_stats(_gold_input(spark,rows)).orderBy("settlement_date").collect()
    assert out[0].price_change is None  
    assert out[1].log_return is None
    # The collision-1 test: a negative-price day must yield log_return None,
    # price_change still populated. Two days, second with systemSellPrice=-5.0.
    ...  # [D] write this one yourself - it's the guard's whole reason to exist


def test_negative_period_count(spark):
    rows = [
        make_raw_row(settlementPeriod=1, systemSellPrice=-5.0),
        make_raw_row(settlementPeriod=2, systemSellPrice=45.0),
    ]
    out = daily_price_stats(_gold_input(spark, rows)).collect()
    assert out[0].negative_period_count == 1