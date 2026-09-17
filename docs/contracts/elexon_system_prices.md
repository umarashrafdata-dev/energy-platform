# Contract: Elexon Insights — system prices
Endpoint: GET /balancing/settlement/system-prices/{settlementDate}
Auth: none · Verified: 2026-09-17 (scratch/explore_elexon.py)

## Envelope
{ "metadata": {...}, "data": [ <row per settlement period> ] }

## Row (observed types)
settlementDate: str "YYYY-MM-DD" · settlementPeriod: int (1..50)
startTime: str ISO-8601 UTC ("Z") — period start, source-provided
createdDateTime: str ISO-8601 UTC — when Elexon produced the record
systemSellPrice / systemBuyPrice: float (equal since single-price regime)
priceDerivationCode: str · bsadDefaulted: bool
netImbalanceVolume + volume fields: float
replacementPrice, replacementPriceReferenceVolume,
totalSystemTaggedAdjustment*Volume: float | null (null is NORMAL)

## Behaviour (observed)
- Normal day: 48 rows · long clock-change day: 50 (2025-10-26) · short: 46 (2025-03-30)
- Current day: PARTIAL — rows appear intra-day (23 rows @ 12:12)
- Future date: 200 + empty data (not an error)
- Malformed date: 400 → do not retry 4xx; retry 5xx/timeouts with backoff
- Same period may be re-served with revised values (settlement reruns) — bronze captures all versions

## Watermark grain
(settlement_date, settlement_period)