import os

# ==================================================
# CONFIGURACIÓN GENERAL
# ==================================================

# AWS
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-south-2")
BUCKET_NAME = "polkadot-rush-imat"

# Datos cripto
EXCHANGE = "binance"
SYMBOL = "DOTUSD"  # Polkadot
INTERVAL = "1D"  # Diario (TradingView)
SOURCE = "tradingview"

# Rango temporal
START_YEAR = 2022
END_YEAR = 2025  # inclusive

# Raíz lógica en S3
BASE_PREFIX = "raw"

# Carpeta local temporal
LOCAL_BASE_DIR = "data"
