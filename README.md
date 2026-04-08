# 🏎️ F1-Pulse
### End-to-End Data Lakehouse — Automated F1 Telemetry & Performance Pipeline on Databricks

---

## 📋 Project Overview

F1-Pulse is a production-grade data engineering pipeline that ingests, transforms, and analyzes Formula 1 racing data using the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline pulls raw JSON responses from the **OpenF1 REST API**, processes them through three Delta Lake layers, and delivers actionable driver and constructor performance insights. It is currently configured to process the **2025 Abu Dhabi Grand Prix**, showcasing retry-safe API ingestion, schema drift detection, window function analytics, and multi-table Gold outputs.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Platform | Databricks (Serverless Compute) |
| Language | Python (PySpark + Pandas) |
| Storage | Delta Lake (ACID transactions, Time Travel, Schema Enforcement) |
| Governance | Unity Catalog (3-layer namespace: `f1_project.bronze/silver/gold`) |
| Version Control | GitHub Integration |
| Orchestration | Databricks Workflows (Jobs) |
| Data Source | OpenF1 REST API (free, real-time F1 telemetry) |

---

## 🏗️ Architecture

```
OpenF1 REST API
      │
      ▼
┌─────────────────────────────────────────┐
│  BRONZE LAYER  (raw_sessions, raw_laps, │
│  raw_drivers, raw_telemetry)            │
│  • Retry-safe ingestion (3 attempts)    │
│  • Pandas middle-man for type safety    │
│  • ingested_at + source_url audit cols  │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  SILVER LAYER  (cleaned_sessions,       │
│  enriched_laps)                         │
│  • Schema drift detection               │
│  • Type casting (no blanket str coerce) │
│  • Driver deduplication before join     │
│  • is_valid_lap quality flag            │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  GOLD LAYER  (driver_performance,       │
│  constructor_standings, lap_progression)│
│  • Window functions & rankings          │
│  • Lap consistency (std deviation)      │
│  • Rolling 5-lap average (time-series)  │
└─────────────────────────────────────────┘
```

---

## 🔬 Layer Details

### Bronze — Raw Ingestion (`01_Ingest_F1_Data`)
- Fetches Sessions, Laps, Drivers, and Telemetry from the OpenF1 API
- **Retry logic**: 3 attempts with 5s delay and 30s timeout per request — resilient to flaky free-tier APIs
- **Smart type handling**: only object/mixed columns are stringified; numeric types (lap times, speeds) are preserved natively
- **Safe session resolution**: filters by `session_type = Race` before selecting latest session key — no fragile `[-1]` index assumptions
- Audit columns: `ingested_at` timestamp + `source_url` on every table
- Idempotent: `overwrite` + `overwriteSchema` mode handles re-runs and schema evolution

### Silver — Cleaning & Enrichment (`02_Silver_Transformation`)
- **Schema drift guard**: `assert_columns()` validates expected fields exist before any transformation — catches API changes immediately
- **Proper type casting**: uses Spark's `IntegerType`, `FloatType`, `BooleanType` explicitly — no inference guesswork
- **Driver deduplication**: `dropDuplicates(["driver_number"])` before the join prevents row multiplication from multi-segment API responses
- **Quality flagging**: introduces `is_valid_lap` boolean column — flags pit-out laps and anomalous durations without dropping data, letting downstream layers decide
- Carries `country_code` and `headshot_url` forward for potential dashboard use

### Gold — Analytics (`03_Gold_Analytics`)
Produces **three purpose-built Delta tables** from a single Silver read:

| Table | Description |
|---|---|
| `driver_performance_metrics` | Per-driver leaderboard: fastest lap, avg pace, median pace, lap consistency (std dev), position rank |
| `constructor_standings` | Team-level summary: best lap, avg team pace, total laps, constructor rank |
| `lap_progression` | Lap-by-lap time series with rolling 5-lap average — ready for line charts |

---

## 📈 Sample Results — 2025 Abu Dhabi Grand Prix

### Driver Leaderboard

| Rank | Driver | Team | Fastest Lap (s) | Avg Pace (s) | Consistency (σ) |
|---|---|---|---|---|---|
| 1 | Charles LECLERC | Ferrari | 86.720 | 88.790 | 1.23 |
| 2 | Oscar PIASTRI | McLaren | 86.760 | 88.980 | 1.31 |
| 3 | Max VERSTAPPEN | Red Bull Racing | 87.620 | 88.750 | 1.18 |
| 4 | Kimi ANTONELLI | Mercedes | 88.020 | 90.200 | 1.67 |

### Constructor Standings

| Rank | Team | Best Lap (s) | Avg Team Pace (s) |
|---|---|---|---|
| 1 | Ferrari | 86.720 | 88.790 |
| 2 | McLaren | 86.760 | 88.980 |
| 3 | Red Bull Racing | 87.620 | 88.750 |
| 4 | Mercedes | 88.020 | 90.200 |

---

## 🚀 Key Engineering Features

- **Retry-safe ingestion**: exponential back-off with configurable retries and timeouts — no silent failures on flaky APIs
- **Schema drift detection**: assertion guards at every layer boundary catch upstream API changes before they corrupt downstream tables
- **Quality flagging over hard-dropping**: `is_valid_lap` preserves all data in Silver; Gold filters only when computing metrics
- **Window function analytics**: `dense_rank()` for leaderboards, `stddev()` for consistency scoring, rolling averages for time-series
- **Idempotent pipelines**: `overwrite` + `overwriteSchema` on all writes — re-runnable with no side effects
- **Structured logging**: timestamped log output with row counts and quality summaries at every step
- **Config abstraction**: all environment constants (`CATALOG`, `SCHEMA`, `YEAR`, timeouts) defined at the top of each notebook — zero hunting through logic
- **CI/CD**: notebooks version-controlled and synced directly from GitHub to Databricks
- **Orchestration**: end-to-end pipeline schedulable via Databricks Workflows

---

## 📂 Repository Structure

```
F1-Pulse/
├── 01_Ingest_F1_Data.ipynb       # Bronze: retry-safe OpenF1 API ingestion
├── 02_Silver_Transformation.ipynb # Silver: schema validation, enrichment, quality flagging
├── 03_Gold_Analytics.ipynb        # Gold: leaderboard, constructor standings, lap progression
└── README.md
```

---

## 🔧 How to Run

1. Import notebooks into a Databricks workspace with Unity Catalog enabled
2. Ensure the `f1_project` catalog and `bronze`, `silver`, `gold` schemas exist (or update `CATALOG`/`SCHEMA` constants at the top of each notebook)
3. Run notebooks in order: `01` → `02` → `03`
4. Optionally configure as a Databricks Workflow for scheduled runs

> No API key required — OpenF1 is a free, open REST API.

---

*Built with ❤️ for Formula 1 & Data Engineering enthusiasts*
