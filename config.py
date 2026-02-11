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

# ==================================================
# GLUE DATA CATALOG
# ==================================================
DATABASE_NAME = "trade_data_imat3a04"
CRAWLER_NAME = "dot_history_crawler"
TABLE_PREFIX = "dot_"
ROLE_ARN = "arn:aws:iam::490004641586:role/Sprint2a04"

# Nombres de tabla en el catálogo (TABLE_PREFIX + nombre de carpeta S3)
BRONZE_TABLE = "dot_bronze"
SILVER_TABLE = "dot_silver"
GOLD_TABLE = "dot_gold"
