# RUSH-POLKA (SPRINT-3)

![Python](https://img.shields.io/badge/Python-3.12.10-blue.svg)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange.svg)
![AWS Glue](https://img.shields.io/badge/AWS-Glue-purple.svg)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C.svg)
![Polkadot](https://img.shields.io/badge/Asset-Polkadot-e6007a.svg)

## Overview

**RUSH-POLKA** is an end-to-end Data Lake pipeline built for a Big Data Processing university course. It extracts historical financial data for **Polkadot (DOT)** from TradingView (Binance exchange), processes it through a three-layer architecture (**Bronze → Silver → Gold**) using **AWS Glue** and **Apache Spark**, and produces technical analysis indicators.

## Architecture

The pipeline follows a **medallion architecture** on AWS S3:

| Layer | Format | Description |
|-------|--------|-------------|
| **Bronze** | CSV | Raw OHLCV data partitioned by `year/month` |
| **Silver** | Parquet | Cleaned, deduplicated, and typed data |
| **Gold** | Parquet | Enriched data with technical indicators (SMA 200, EMA 50, RSI, MACD) |

Data is catalogued in **AWS Glue Data Catalog** (`trade_data_imat3a04` database) using a Crawler, and each Glue Job reads from the catalog rather than directly from S3.

```text
TradingView API ──► Bronze (CSV) ──► Silver (Parquet) ──► Gold (Parquet + KPIs)
                    deploy_bronce    deploy_silver         deploy_gold
```

## Features

- **Historical Data Extraction**: Fetches OHLCV data from TradingView via WebSocket.
- **Three-Layer Data Lake**: Bronze (raw), Silver (clean), Gold (analytics-ready).
- **AWS Glue Integration**: Crawler indexes all layers in the Data Catalog; Glue Jobs (PySpark) perform ETL.
- **Technical Indicators**: SMA 200, EMA 50, RSI (14), MACD (12/26) computed via distributed Pandas UDFs.
- **Centralized Configuration**: All parameters managed in a single `config.py`.

## Project Structure

```text
RUSH-POLKA/
├── config.py                  # Centralized configuration (AWS, symbol, S3 paths, Glue Catalog)
├── utils.py                   # Helper functions (S3 ops, CSV writing, path building)
├── crawler.py                 # AWS Glue Crawler & Data Catalog management
│
├── deploy_bronce.py           # Step 1: Fetch data from TradingView → upload CSVs to Bronze
├── deploy_silver.py           # Step 2: Run Crawler + Glue Job (Bronze → Silver as Parquet)
├── deploy_gold.py             # Step 3: Run Crawler + Glue Job (Silver → Gold with KPIs)
│
├── glue_bronze_to_silver.py   # PySpark script: read Bronze from catalog, clean, write Parquet
├── glue_silver_to_gold.py     # PySpark script: read Silver from catalog, compute KPIs, write Parquet
│
├── create_structure.py        # Utility: creates bronze/, silver/, gold/ folders in S3
├── TradingviewData/           # Third-party module for TradingView WebSocket API
├── data/                      # Local temporary data (auto-generated, not committed)
├── LICENSE
└── README.md
```

### S3 Bucket Structure

```text
s3://polkadot-rush-imat/
├── bronze/
│   └── year=YYYY/month=MM/
│       └── DOTUSD_binance_1D_YYYY-MM.csv
├── silver/
│   └── year=YYYY/month=MM/
│       └── *.parquet
├── gold/
│   └── year=YYYY/month=MM/
│       └── *.parquet
├── scripts/
│   ├── glue_bronze_to_silver.py
│   └── glue_silver_to_gold.py
└── tmp/
```

## Prerequisites

- **Python 3.8+**
- **AWS Account** with access to S3, Glue, and IAM.
- AWS credentials available as environment variables.

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repo_url>
   cd RUSH-POLKA
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # Linux/Mac
   source .venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install pandas boto3 websocket-client matplotlib pyarrow
   ```

## Configuration

All settings are managed in `config.py`.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BUCKET_NAME` | `polkadot-rush-imat` | S3 bucket for the Data Lake |
| `AWS_REGION` | `eu-south-2` | AWS region |
| `SYMBOL` | `DOTUSD` | Crypto pair to fetch |
| `START_YEAR` | `2022` | Start year for data extraction |
| `END_YEAR` | `2025` | End year for data extraction |
| `DATABASE_NAME` | `trade_data_imat3a04` | Glue Data Catalog database |
| `ROLE_ARN` | `arn:aws:iam::490004641586:role/Sprint2a04` | IAM role for Glue jobs |

## Usage

All commands must be run from the `RUSH-POLKA/` directory.

### 1. Set AWS Credentials

```powershell
$env:AWS_ACCESS_KEY_ID = "your_key"
$env:AWS_SECRET_ACCESS_KEY = "your_secret"
$env:AWS_SESSION_TOKEN = "your_token"   # If using temporary credentials
$env:AWS_DEFAULT_REGION = "eu-south-2"
```

### 2. Run the Pipeline (in order)

```powershell
# Step 1: Ingest raw data → Bronze (CSV)
python deploy_bronce.py

# Step 2: Bronze → Silver (Parquet, cleaned)
python deploy_silver.py

# Step 3: Silver → Gold (Parquet + technical indicators)
python deploy_gold.py
```

> Each step must complete before the next one starts. Steps 2 and 3 run a Glue Crawler and a Glue Job, which take a few minutes.

### Technical Indicators (Gold Layer)

| Indicator | Description |
|-----------|-------------|
| **SMA 200** | Simple Moving Average — 200 periods |
| **EMA 50** | Exponential Moving Average — 50 periods |
| **MACD** | Moving Average Convergence Divergence (EMA12 − EMA26) |
| **RSI** | Relative Strength Index — 14 periods (Wilder's smoothing) |

## Troubleshooting

- **NoCredentialsError**: Ensure credentials are exported in your current PowerShell session.
- **FileNotFoundError**: Make sure you run scripts from the `RUSH-POLKA/` directory, not from subdirectories.
- **ImportError (websocket)**: Install the correct package:
  ```bash
  pip uninstall websocket
  pip install websocket-client
  ```
- **Glue Job fails**: Check the error message printed by the deploy script. Common issues include incorrect IAM role permissions or empty source tables.

## Credits

- The `TradingviewData` module is based on [TradingView-Data](https://github.com/ravalmeet/TradingView-Data) by ravalmeet.

## License

University Project — 3rd Year, Big Data Processing Technologies.

Proyecto Universitario — 3er Año, Tecnologías de Procesamiento Big Data.