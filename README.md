# G2F Platform (Greyhound to Betting Simulator Competition Winner)

Enterprise-grade automated data pipeline and machine learning system for predicting UK/Irish Greyhound races.

The project aim to create a best profitable Machine Learning and AI/LLM solution to create a system able to win competition in simulated real world greyhound betting challenge

## Architecture
Strictly enforces **Domain-Driven Design (DDD)** and **Hexagonal Architecture**.
Follows the **Medallion Data Architecture**:
*   **Bronze:** Immutable raw JSON scraped via Playwright (stored in GCS).
*   **Silver:** Normalized PostgreSQL Fact Store (strict Python native types).
*   **Gold:** Denormalized Feature Store for ML (XGBoost/LLM).

## Tech Stack
*   **Language:** Python 3.12+ (Strict Mypy, Ruff)
*   **Database:** PostgreSQL (SQLAlchemy 2.0, Alembic, PGVector)
*   **Scraping:** Playwright (Async/Sync, Stateful WAF Evasion)
*   **Cloud:** Google Cloud Storage, GitHub Actions (Shadow Orgs)

## Documentation & Operations
All operational procedures are documented in the Runbooks:
*   [Harvester Runbook](RUNBOOKS/RUNBOOK_HARVESTER.md) - Cloud Scraping, WAF evasion, GitHub Actions.
*   [Data Ops Runbook](RUNBOOKS/RUNBOOK_DATA_OPS.md) - Local syncing, Silver ingestion, database audits.
*   [Weather Runbook](RUNBOOKS/RUNBOOK_WEATHER.md) - Open-Meteo pipeline and actuals/forecast sync.
*   [Infra Runbook](RUNBOOKS/RUNBOOK_INFRA.md) - Terraform and GCP state management.
