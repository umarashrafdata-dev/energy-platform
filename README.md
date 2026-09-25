# energy-platform

Production-grade GB energy data platform on Databricks: watermark-driven API
ingestion (Python wheel + Asset Bundles) → Lakeflow Declarative Pipelines
medallion (silver dedupe, gold quant analytics) → DQX quality gates with
quarantine → CI/CD via GitHub Actions.

Full write-up in progress. Built test-first: the entire pipeline logic runs
locally under pytest before it ever touches a workspace.