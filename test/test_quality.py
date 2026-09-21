import pytest

from energy_platform.quality import (
    BatchQualityError,
    enforce_batch_threshold,
    split_on_quality,
)
from energy_platform.transforms import RAW_SCHEMA, standardise_prices
from test_transforms import make_raw_row

from unittest.mock import MagicMock
from databricks.sdk import WorkspaceClient


@pytest.fixture(scope="session")
def ws():
    return MagicMock(spec=WorkspaceClient)


def _standardised(spark, rows: list[dict]):
    return standardise_prices(spark.createDataFrame(rows, schema=RAW_SCHEMA))


# --- Row verdicts -----------------------------------------------------------

def test_clean_rows_all_pass(spark, ws):
    good, quarantined = split_on_quality(_standardised(spark, [make_raw_row()]), ws=ws)
    assert good.count() == 1
    assert quarantined.count() == 0


def test_out_of_range_period_is_quarantined(spark, ws):
    df = _standardised(spark, [make_raw_row(), make_raw_row(settlementPeriod=99)])
    good, quarantined = split_on_quality(df, ws=ws)
    assert good.count() == 1
    assert quarantined.count() == 1
    assert quarantined.collect()[0].settlement_period == 99


def test_null_price_is_quarantined(spark, ws):
    df = _standardised(spark, [make_raw_row(systemSellPrice=None)])
    good, quarantined = split_on_quality(df, ws=ws)
    assert good.count() == 0
    assert quarantined.count() == 1


def test_diverged_prices_warn_but_pass(spark, ws):
    df = _standardised(spark, [make_raw_row(systemBuyPrice=99.99)])
    good, quarantined = split_on_quality(df, ws=ws)
    assert good.count() == 1          # warn tier: flagged, never diverted
    assert quarantined.count() == 0


def test_quarantined_row_names_its_rule(spark,ws):
    df = _standardised(spark, [make_raw_row(settlementPeriod=99)])
    _, quarantined = split_on_quality(df, ws=ws)
    annotations = str(quarantined.collect()[0].asDict())
    assert "settlement_period_in_range" in annotations


# --- Run verdict (pure Python — no Spark) ------------------------------------

def test_threshold_passes_under_limit():
    enforce_batch_threshold(good_count=95, quarantined_count=5)   # 5% — fine


def test_threshold_fails_over_limit():
    with pytest.raises(BatchQualityError):
        enforce_batch_threshold(good_count=70, quarantined_count=30)   # 30%


def test_threshold_handles_empty_batch():
    enforce_batch_threshold(good_count=0, quarantined_count=0)    # no divide-by-zero