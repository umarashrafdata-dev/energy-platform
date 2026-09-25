from energy_platform.analytics import daily_price_stats, price_spikes, rolling_volatility
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



def _daily(spark, rows):
    return daily_price_stats(_gold_input(spark, rows))


def _three_days(spark):
    """Days at mean prices 40, 50, 60 - hand-checkable rolling arithmetic."""
    rows = [
        make_raw_row(settlementDate="2026-09-15", settlementPeriod=1, systemSellPrice=40.0),
        make_raw_row(settlementDate="2026-09-16", settlementPeriod=1, systemSellPrice=50.0),
        make_raw_row(settlementDate="2026-09-17", settlementPeriod=1, systemSellPrice=60.0),
    ]
    return _daily(spark, rows)


def test_rolling_mean_is_trailing_window(spark):
    out = rolling_volatility(_three_days(spark)).orderBy("settlement_date").collect()
    assert float(out[0].rolling_mean_7d) == 40.0          # window of one
    assert float(out[1].rolling_mean_7d) == 45.0          # (40+50)/2
    assert float(out[2].rolling_mean_7d) == 50.0          # (40+50+60)/3


def test_first_day_vol_and_zscore_are_null(spark):
    out = rolling_volatility(_three_days(spark)).orderBy("settlement_date").collect()
    assert out[0].rolling_vol_7d is None                  # stddev needs 2 changes
    assert out[0].price_zscore is None                    # guard held


def test_flat_series_zero_vol_guard(spark):
    rows = [
        make_raw_row(settlementDate=f"2026-09-{d}", settlementPeriod=1, systemSellPrice=50.0)
        for d in ("15", "16", "17")
    ]
    out = rolling_volatility(_daily(spark, rows)).orderBy("settlement_date").collect()
    assert out[2].price_zscore is None                    # vol == 0 -> guarded, no div-by-zero


def test_calm_series_yields_no_spikes(spark):
    rows = [
        make_raw_row(settlementDate="2026-09-15", settlementPeriod=1, systemSellPrice=40.0),
        make_raw_row(settlementDate="2026-09-16", settlementPeriod=1, systemSellPrice=50.0),
        make_raw_row(settlementDate="2026-09-17", settlementPeriod=1, systemSellPrice=60.0),
    ]
    silver = _gold_input(spark, rows)
    vol = rolling_volatility(daily_price_stats(silver))
    assert price_spikes(silver, vol, k=3.0).count() == 0


def test_extreme_price_is_flagged(spark):
    rows = [
        make_raw_row(settlementDate=f"2026-09-{d:02d}", settlementPeriod=1, systemSellPrice=p)
        for d, p in [(10, 48.0), (11, 52.0), (12, 49.0), (13, 51.0), (14, 50.0)]
    ]
    # Day 6: one tame period and one absurd one - the daily mean shifts a bit,
    # but the 500 period sits miles above the trailing stats.
    rows += [
        make_raw_row(settlementDate="2026-09-15", settlementPeriod=1, systemSellPrice=50.0),
        make_raw_row(settlementDate="2026-09-15", settlementPeriod=2, systemSellPrice=500.0),
    ]
    silver = _gold_input(spark, rows)
    vol = rolling_volatility(daily_price_stats(silver))

    flagged = price_spikes(silver, vol, k=3.0).collect()
    assert len(flagged) == 1
    assert flagged[0].settlement_period == 2
    assert flagged[0].spike_zscore > 3.0