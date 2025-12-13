# Kasparro Backend Assignment - Cryptocurrency ETL Pipeline
### Submitted by : Sumit Kumar Verma

**Status:** 🟢 Complete & Deployed  
**Deployment:** Google Cloud Run (Serverless)  
**CI/CD:** GitHub Actions (Automated Testing & Deployment)

**Deployed-URL** - [kasparro-etl-backend-758785475066.us-central1.run.app/docs#/](https://kasparro-etl-backend-758785475066.us-central1.run.app/docs#/)

---
### Deployment Artifact Location Note

**The Docker image is managed in Google Artifact Registry (GAR) as a best practice for GCP-native deployments, rather than Docker Hub.**

The full image path is: `us-central1-docker.pkg.dev/kasparro-etl-project/kasparro-repo/kasparro-backend:latest`

This choice is made for security, performance, and seamless integration with Google Cloud Run.

---
## 🏗️ Architecture Overview

This project implements a robust, scalable ETL pipeline using **Clean Architecture** principles to ensure separation of concerns and testability. It aggregates real-time cryptocurrency market data from multiple disparate sources (CoinPaprika, CoinGecko, CSV file) into a unified analytical schema.

### The Pipeline Flow
1.  **Ingestion Layer (`app/ingestion`)**: Data is fetched asynchronously from live APIs (CoinPaprika, CoinGecko) and legacy CSV imports.
2.  **Normalization Layer (`app/schemas`)**: Raw payloads are validated and transformed into a strict `CryptoUnifiedData` schema using Pydantic.
3.  **Storage Layer (`app/db`)**: Normalized data is upserted into PostgreSQL (Cloud SQL) with conflict resolution (**idempotency**) to handle duplicates.
4.  **API Layer (`app/api`)**: A high-performance FastAPI interface provides access to market data, metrics, and trigger controls.

### Architecture Diagram
```mermaid
graph TD
    subgraph Sources
        CP[CoinPaprika API]
        CG[CoinGecko API]
        CSV[CSV Imports]
    end

    subgraph "Kasparro ETL System"
        Scheduler[Cloud Scheduler] -->|Trigger /etl/run| API_Gateway[FastAPI]
        User[User/Client] -->|Query Data /data| API_Gateway
        
        API_Gateway -->|Async Task| Ingestion[Ingestion Layer]
        
        Ingestion -->|Fetch Async| Sources
        
        Ingestion --> Drift[Drift Detection]
        Drift -->|Valid?| Schema["Normalization (Pydantic)"]
        
        Schema -->|"Upsert (Idempotent)"| DB[(PostgreSQL)]
        
        API_Gateway -->|Read| DB
    end
```
---
## 💪 Key Features (The "Flex" Section)

Beyond the baseline requirements (P0+P1), this submission includes several advanced **"Level 2"** differentiators:

### 1. Schema Drift Detection 🛡️
Before ingestion, the `drift_detection` service scans incoming API payloads. If new or unexpected keys appear (indicating an upstream API change), the system logs structured warnings. This allows proactive alerts before data quality degrades.
- **Code:** `app/services/drift_detection.py`

### 2. Resilience Pattern: Chaos Mode 💥 & Backoff 🔄
* **Exponential Backoff:** API sources use `tenacity` decorators to gracefully handle rate limits (HTTP 429) and transient network failures.
* **Chaos Engineering:** A built-in `CHAOS_MODE` (controlled by env var) intentionally simulates mid-stream failures. The system is proven to log the error, save the failure checkpoint, and resume correctly from the last checkpoint on the next run.
- **Code:** `app/ingestion/pipeline.py` (Retry decorators & Chaos logic)

### 3. State Management & Observability 📊
* **Checkpointing:** The `etl_checkpoints` table tracks every run's status, duration, and record count. The pipeline checks this state to ensure incremental processing.
* **Prometheus Metrics:** Custom metrics (`etl_records_processed_total`, `etl_run_duration_seconds`) are exposed at `/metrics` for scraping.
- **Code:** `app/api/routes.py` (/stats endpoint with detailed ETL summaries)

### 4. Fully Automated CI/CD 🚀
A GitHub Actions pipeline (`.github/workflows/ci.yml`) enforces quality:
* **CI:** Runs `pytest` (with asyncio event loop handling) on every push.
* **CD:** Automatically builds the Docker image, pushes to Google Artifact Registry, and deploys to Cloud Run upon merging to `main`.

---

## 🛠️ Tech Stack

* **Language:** Python 3.11 (Type-hinted)
* **Framework:** FastAPI (High performance Async I/O)
* **Database:** PostgreSQL 15 (SQLAlchemy Async ORM + AsyncPG)
* **Infrastructure:** Docker, Google Cloud Run, Cloud Scheduler, Artifact Registry.
* **Testing:** Pytest, Pytest-Asyncio.

---

## 🚀 Setup Instructions

### 1. Environment Setup
Create a `.env` file in the root directory:

```bash
# Database Configuration
DATABASE_URL=postgresql+asyncpg://user:password@hostname:5432/dbname
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=your_database_name

# API Keys (Required for P1 Source)
COINGECKO_API_KEY=your_api_key_here

# Resilience
CHAOS_MODE=False
```
### 2. Running Locally (Docker)
Your system must run entirely via the provided make commands.

Start System:

```bash
make up
# OR
docker-compose up --build -d
```
Run Pipeline Manually (Smoke Test):

```bash
docker-compose exec backend python -m app.ingestion.pipeline
```
Run Tests:

```bash
make test
# OR
docker-compose exec backend pytest -v
```
Stop System:

```bash
make down
```

## ☁️ Deployment Guide

This repository is configured for **Zero-Touch Deployment**.

1.  **Push to Main:** Any commit pushed to the `main` branch triggers the pipeline.
2.  **Automated Testing:** GitHub Actions runs the test suite.
3.  **Deployment:** On success, the image is deployed to Google Cloud Run.
4.  **Secrets:** The pipeline uses GitHub Secrets (`GCP_SA_KEY`, `PROD_DATABASE_URL`, `COINGECKO_API_KEY`) to configure the production environment securely.

---

## 📡 API Documentation

Once running (Local: `http://localhost:8000/docs`, Prod: [Live API Docs](https://kasparro-etl-backend-758785475066.us-central1.run.app/docs#/)), the following endpoints are available:

### Core Data
* `GET /health`: System health check (includes DB connectivity status and ETL last-run status).
* `GET /data`: Retrieve unified crypto market data (supports limit, offset, and ticker filters).

### Observability & OPS
* `GET /stats`: Detailed history of ETL runs, status, and error logs.
* `GET /metrics`: Prometheus formatted metrics.
* `GET /runs` & `/compare-runs`: Advanced run comparison analysis.

### Trigger
* `POST /etl/run`: Manually trigger the ETL pipeline (used by Cloud Scheduler).

---

_Built with ❤️ by Sumit Verma for Kasparro Evaluation._
