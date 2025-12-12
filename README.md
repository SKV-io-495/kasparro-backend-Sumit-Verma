# Cryptocurrency Market ETL Pipeline

This project is a production-grade ETL (Extract, Transform, Load) pipeline that aggregates cryptocurrency market data from multiple sources into a unified schema.

## Data Sources
*   **CoinPaprika (Live API)**: Real-time market metrics.
*   **CoinGecko (Live API)**: Extensive coin covereage and historical data.
*   **CSV Ingestion**: Support for manual backfills and legacy data imports.

## Architecture
The system follows a modern ETL architecture:
1.  **Ingestion Layer**: Fetches data from external APIs using `tenacity` for resilient retries.
2.  **normalization Layer**: Converts disparate upstream formats into a **Unified Crypto Schema**.
3.  **Storage Layer**: Persists clean data into PostgreSQL with time-series optimization.
4.  **API Layer**: Exposes data via FastAPI for downstream consumption.

## Setup

### Prerequisites
*   Python 3.10+
*   PostgreSQL
*   Docker (Optional)

### Environment Variables
Create a `.env` file with the following required variables:

```bash
DATABASE_URL=postgresql+asyncpg://user:password@localhost/dbname
COINGECKO_API_KEY=your_api_key_here
```

### Installation
```bash
pip install -r requirements.txt
```

### Running the Pipeline
```bash
python -m app.main
```
