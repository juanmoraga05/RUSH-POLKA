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

# Carpeta local temporal
LOCAL_BASE_DIR = "data"

# ==================================================
# CAPAS DEL DATA LAKE (Bronze / Silver / Gold)
# ==================================================
BRONZE_LOCAL_DIR = os.path.join(LOCAL_BASE_DIR, "bronze")
SILVER_LOCAL_DIR = os.path.join(LOCAL_BASE_DIR, "silver")
GOLD_LOCAL_DIR = os.path.join(LOCAL_BASE_DIR, "gold")

# Prefijos S3 por capa
BRONZE_S3_PREFIX = "bronze"
SILVER_S3_PREFIX = "silver"
GOLD_S3_PREFIX = "gold"
