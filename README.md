# Kasparro ETL Backend - Senior Engineer Submission

**Status:** 🟢 Complete & Deployed  
**Deployment:** Google Cloud Run (Serverless)  
**CI/CD:** GitHub Actions (Automated Testing & Deployment)

**Deployed-link** - [kasparro-etl-backend-758785475066.us-central1.run.app/docs#/](https://kasparro-etl-backend-758785475066.us-central1.run.app/docs#/)

---

## 🏗️ Architecture Overview

This project implements a robust, scalable ETL pipeline using **Clean Architecture** principles to ensure separation of concerns and testability.

### The Pipeline Flow
1.  **Ingestion Layer (`app/ingestion`)**: Data is fetched asynchronously from multiple sources (Mock API, CSV File, RSS Feed).
2.  **Normalization Layer (`app/schemas`)**: Raw data is validated and transformed into a strict `UnifiedData` schema using Pydantic.
3.  **Storage Layer (`app/db`)**: Normalized data is upserted into PostgreSQL (Cloud SQL) with conflict resolution (idempotency).
4.  **API Layer (`app/api`)**: A high-performance FastAPI interface provides access to data, metrics, and trigger controls.

---

## 💪 Key Features (The "Flex" Section)

Beyond the baseline requirements, this submission includes several advanced "Level 2" differentiators:

### 1. Schema Drift Detection 🛡️
Before ingestion, the `drift_detection` service scans incoming payloads. If new or unexpected keys appear (indicating an upstream API change), the system logs structured warnings with the specific drift details, allowing potential alerts *before* data quality degrades.
*   *Code:* `app/services/drift_detection.py`

### 2. Resilience Pattern: Chaos Mode 💥 & Backoff 🔄
*   **Exponential Backoff**: API sources use `tenacity` decorators to gracefully handle transient network failures (retrying with jitter).
*   **Chaos Engineering**: A built-in `CHAOS_MODE` (controlled by env var) intentionally simulates mid-stream failures. The system is proven to log the error, save the failure checkpoint, and resume correctly on the next run.
*   *Code:* `app/ingestion/pipeline.py` (Retry decorators & Chaos logic)

### 3. State Management & Observability 📊
*   **Checkpointing**: The `etl_checkpoints` table tracks every run's status, duration, and record count. The pipeline checks this state to ensure incremental processing.
*   **Prometheus Metrics**: Custom metrics (`etl_records_processed_total`, `etl_run_duration_seconds`) are exposed at `/metrics` for scraping.
*   *Code:* `app/api/routes.py` (`/stats`)

### 4. Fully Automated CI/CD 🚀
A GitHub Actions pipeline (`.github/workflows/ci.yml`) enforces quality:
*   **CI**: Runs `pytest` (with asyncio event loop handling) on every push.
*   **CD**: Automatically builds the Docker image, pushes to **Google Artifact Registry**, and deploys to **Cloud Run** upon merging to `main`.

---

## 🛠️ Tech Stack

*   **Language**: Python 3.11 (Type-hinted)
*   **Framework**: FastAPI (High performance Async I/O)
*   **Database**: PostgreSQL 15 (SQLAlchemy Async ORM + AsyncPG)
*   **Infrastructure**: Docker, Google Cloud Run, Cloud Scheduler, Artifact Registry.
*   **Testing**: Pytest, Pytest-Asyncio.

---

## 🚀 Setup Instructions

### 1. Environment Setup
Create a `.env` file in the root directory:
```bash
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/kasparro
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=kasparro
CHAOS_MODE=False
```

### 2. Running Locally (Docker)
We use a convenient optional `Makefile` or direct Docker Compose commands.

**Start System:**
```bash
make up
# OR
docker-compose up --build
```

**Run Tests:**
```bash
make test
# OR
docker-compose run backend pytest -v
```

**Stop System:**
```bash
make down
```

---

## ☁️ Deployment Guide

This repository is configured for **Zero-Touch Deployment**.

1.  **Push to Main**: Any commit pushed to the `main` branch triggers the pipeline.
2.  **Automated Testing**: GitHub Actions runs the test suite.
3.  **Deployment**: On success, the image is deployed to **Google Cloud Run**.
4.  **Secrets**: The pipeline uses GitHub Secrets (`GCP_SA_KEY`, `PROD_DATABASE_URL`, `API_KEY`) to configure the production environment securely.

---

## 📡 API Documentation

Once running (Local: `http://localhost:8000`, Prod: `https://[YOUR-CLOUD-RUN-URL]`), the following endpoints are available:

### Core Data
*   `GET /health`: System health check (includes DB connectivity status).
*   `GET /data`: Retrieve unified data (supports `limit`, `offset`, and `category` filters).

### Observability & OPS
*   `GET /stats`: Detailed history of ETL runs, status, and error logs.
*   `GET /metrics`: Prometheus formatted metrics.
*   `GET /runs` & `/compare-runs`: Advanced run comparison analysis.

### Trigger
*   `POST /etl/run`: Manually trigger the ETL pipeline (used by Cloud Scheduler).

---

*Built with ❤️ by Sumit Verma for Kasparro Evaluation.*
