# 🏎️ F1-Pulse

**End-to-End Data Lakehouse Automated F1 Telemetry & Performance Pipeline on Databricks**

## 📋 Project Overview

F1-Pulse is a production-grade data engineering pipeline that ingests, transforms, and analyzes Formula 1 racing data using the **Medallion Architecture**.

The pipeline pulls raw JSON responses from the **OpenF1 API**, processes them through Bronze → Silver → Gold layers, and delivers actionable driver performance insights. 

It is currently configured to process the **2025 Abu Dhabi Grand Prix**, showcasing complex joins, sensor telemetry handling, and historical race analysis.

## 🛠️ Tech Stack

- **Platform**: Databricks (Serverless Compute)
- **Language**: Python (PySpark + Pandas)
- **Storage**: Delta Lake (ACID transactions, Time Travel, Schema Enforcement)
- **Governance**: Unity Catalog (3-layer namespace)
- **Version Control**: GitHub Integration
- **Orchestration**: Databricks Workflows (Jobs)

## 🏗️ Architecture Layers

### 1. Bronze Layer (Raw Ingestion)
- **Source**: OpenF1 REST API
- **Method**: Python `requests` + Pandas-to-Spark conversion for type safety
- **Storage**: Raw Delta tables for Sessions, Laps, Drivers, and Telemetry
- **Audit**: `ingested_at` timestamp added to all records

### 2. Silver Layer (Cleaned & Enriched)
- Schema enforcement and type casting (strings → timestamps, floats, booleans)
- Enrichment through inner joins between Laps and Drivers tables (driver number → full name + constructor team)
- Quality improvements: Filtered out "Pit Out" laps for accurate timing analysis

### 3. Gold Layer (Aggregated Insights)
- Aggregated metrics: Fastest Lap, Average Lap Pace, Total Valid Laps per driver
- Produces a clean, dashboard-ready leaderboard with performance insights across the 2025 grid

## 📈 Sample Results – 2025 Abu Dhabi Grand Prix

| Team                | Driver            | Fastest Lap (s) | Avg Lap Time (s) |
|---------------------|-------------------|-----------------|------------------|
| Ferrari            | Charles LECLERC   | 86.72           | 88.79            |
| McLaren            | Oscar PIASTRI     | 86.76           | 88.98            |
| Red Bull Racing    | Max VERSTAPPEN    | 87.62           | 88.75            |
| Mercedes           | Kimi ANTONELLI    | 88.02           | 90.20            |

## 🚀 Key Engineering Features

- **CI/CD Integration**: Notebooks are version-controlled and synced directly from GitHub
- **Idempotent Pipelines**: Uses overwrite logic to prevent duplicate data on re-runs
- **Robust Error Handling**: "Safe Ingestion" patterns for null values and dynamic API schemas
- **Full Automation**: Orchestrated via Databricks Workflows (supports scheduled runs)

## 📂 Repository Structure

- **`01_Ingest_F1_Data`** – Connects to OpenF1 API and populates the Bronze layer
- **`02_Silver_Transformation`** – Cleans, casts types, and joins data into the Silver layer
- **`03_Gold_Analytics`** – Generates final aggregated driver performance metrics for the Gold layer

---

**Built with ❤️ for Formula 1 & Data Engineering enthusiasts**
