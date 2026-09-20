"""Data quality rules and checkpoint logic. Rules trace to
docs/contracts/elexon_system_prices.md — the contract's enforcement arm."""
import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

BRONZE_PRICE_RULES = [
    {"name": "settlement_period_in_range", "criticality": "error",
     "check": {"function": "is_in_range",
               "arguments": {"column": "settlement_period", "min_limit": 1, "max_limit": 50}}},
    {"name": "settlement_date_not_null", "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "settlement_date"}}},
    {"name": "settlement_period_not_null", "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "settlement_period"}}},
    {"name": "start_time_not_null", "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "start_time_utc"}}},
    {"name": "sell_price_not_null", "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "system_sell_price"}}},
    {"name": "buy_price_not_null", "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "system_buy_price"}}},
    {"name": "single_price_regime_holds", "criticality": "warn",
     "check": {"function": "sql_expression",
               "arguments": {"expression": "system_sell_price = system_buy_price",
                             "msg": "sell/buy diverge: single-price regime assumption broken"}}},
]

QUARANTINE_RATE_THRESHOLD = 0.20


class BatchQualityError(Exception):
    """Raised when a batch breaches the systemic quarantine-rate threshold."""


def split_on_quality(
    df: DataFrame, ws: WorkspaceClient | None = None
) -> tuple[DataFrame, DataFrame]:
    """Apply bronze rules; return (good, quarantined)."""
    if ws is None:
        ws = WorkspaceClient()   # on Databricks: ambient auth, real client
    engine = DQEngine(ws, spark=df.sparkSession)
    good, flagged = engine.apply_checks_by_metadata_and_split(df, BRONZE_PRICE_RULES)
    quarantined = flagged.where(F.col("_errors").isNotNull())
    return good, quarantined


def enforce_batch_threshold(good_count: int, quarantined_count: int) -> None:
    """The systemic-breach gate: fail loudly if too much of a batch is bad."""
    total = good_count + quarantined_count
    if total == 0:
        return
    rate = quarantined_count / total
    if rate > QUARANTINE_RATE_THRESHOLD:
        raise BatchQualityError(
            f"Quarantine rate {rate:.0%} exceeds {QUARANTINE_RATE_THRESHOLD:.0%} "
            f"({quarantined_count}/{total} rows) — systemic contract breach suspected"
        )
    logger.info("Quality gate: %d good, %d quarantined (%.1f%%)", good_count, quarantined_count, rate * 100)