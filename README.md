# RUSH-POLKA

![Python](https://img.shields.io/badge/Python-3.12.10-blue.svg)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange.svg)
![Polkadot](https://img.shields.io/badge/Asset-Polkadot-e6007a.svg)

## Overview

**RUSH-POLKA** is a data ingestion pipeline designed for a Big Data processing course. It automates the extraction of historical financial data for **Polkadot (DOT)** from TradingView (Binance exchange), processes it into monthly partitions, and uploads it to an **AWS S3** Data Lake.

This project serves as the raw data acquisition layer (Sprint 1/2), enabling downstream analysis and processing tasks.

## Features

- **Historical Data Extraction**: Connects to TradingView via WebSocket to fetch OHLCV (Open, High, Low, Close, Volume) data.
- **Data Partitioning**: Splits the continuous time series into monthly CSV files.
- **Cloud Ingestion**: Automatically uploads partitioned files to an AWS S3 bucket using `boto3`.
- **Configurable**: Centralized configuration for dates, symbols, and AWS settings.

## Project Structure

```text
RUSH-POLKA/
├── TradingviewData/    # Module to interact with TradingView WebSocket API
├── config.py           # Configuration parameters (AWS, Dates, Symbol)
├── main.py             # Main execution script
├── utils.py            # Helper functions (S3 logic, CSV writing)
└── README.md           # Project documentation
```

## Prerequisites

- **Python 3.8+**
- **AWS Account** with S3 write access.
- **AWS CLI** configured or credentials available in environment variables.

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
   pip install pandas boto3 websocket-client
   ```

## Configuration

Settings are managed in `config.py`.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BUCKET_NAME` | `polkadot-rush-imat` | S3 Bucket name for storage |
| `AWS_REGION` | `eu-south-2` | AWS Region |
| `SYMBOL` | `DOTUSD` | Crypto pair to fetch |
| `START_YEAR` | `2022` | Start year for processing |
| `END_YEAR` | `2025` | End year for processing |

## Usage

1. **Set AWS Credentials**:
   Ensure your session has access to AWS. You can set them temporarily in the terminal or use a profile.
   ```powershell
   $env:AWS_ACCESS_KEY_ID="your_key"
   $env:AWS_SECRET_ACCESS_KEY="your_secret"
   $env:AWS_SESSION_TOKEN="your_token" # If using temporary credentials
   ```

2. **Run the pipeline**:
   ```bash
   python main.py
   ```

3. **Output**:
   - The script will print the status of each processed month.
   - Files will be uploaded to S3 under the prefix structure: `YYYY/MM/`.

## Troubleshooting

- **NoCredentialsError**: Ensure your AWS credentials are exported correctly in your shell or stored in `~/.aws/credentials`.
- **ImportError (websocket)**: If you see issues with the websocket library, ensure you have installed `websocket-client` and not the `websocket` package.
  ```bash
  pip uninstall websocket
  pip install websocket-client
  ```

## Credits

- The `TradingviewData` module used for WebSocket communication is based on a [GitHub repository](https://github.com/ravalmeet/TradingView-Data) (Credits to the original author).

## License

University Project - (3rd Year) Big Data Processing.

Proyecto Universitario - (3er Año) Tecnologías de Procesamiento Big Data