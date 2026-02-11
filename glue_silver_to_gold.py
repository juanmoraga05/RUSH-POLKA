"""
AWS Glue Job: Silver → Gold
Lee los Parquet de la capa Silver, calcula indicadores técnicos
(SMA 200, EMA 50, RSI, MACD) y almacena los resultados en la capa Gold.

Uso: subir este script a S3 y crear un Glue Job que lo ejecute.
"""

import sys
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ==================================================
# INICIALIZACIÓN GLUE
# ==================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ==================================================
# CONFIGURACIÓN
# ==================================================
BUCKET = "polkadot-rush-imat"
SILVER_PATH = f"s3://{BUCKET}/silver/"
GOLD_PATH = f"s3://{BUCKET}/gold/"

# ==================================================
# 1. LECTURA — Parquet desde Silver
# ==================================================
print(f"[INFO] Leyendo Parquet desde {SILVER_PATH}")

df_silver = spark.read.parquet(SILVER_PATH)

print(f"[INFO] Registros leídos desde Silver: {df_silver.count()}")
df_silver.printSchema()

# ==================================================
# 2. CÁLCULO DE INDICADORES TÉCNICOS (Pandas UDF)
# ==================================================
# Columnas originales de Silver (sin particiones Spark)
silver_columns = [c for c in df_silver.columns if c not in ("year", "month")]


def calcular_kpis(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula SMA 200, EMA 50, MACD y RSI sobre un grupo de datos
    agrupado por símbolo.
    """
    # Ordenar cronológicamente
    pdf = pdf.sort_values("datetime")

    # a) SMA 200 — Media móvil simple de 200 periodos
    pdf["SMA_200"] = pdf["close"].rolling(window=200, min_periods=1).mean()

    # b) EMA 50 — Media móvil exponencial de 50 periodos
    pdf["EMA_50"] = pdf["close"].ewm(span=50, adjust=False).mean()

    # c) MACD — Convergencia/divergencia de medias móviles (EMA12 - EMA26)
    ema_12 = pdf["close"].ewm(span=12, adjust=False).mean()
    ema_26 = pdf["close"].ewm(span=26, adjust=False).mean()
    pdf["MACD"] = ema_12 - ema_26

    # d) RSI — Relative Strength Index (14 periodos, suavizado Wilder)
    delta = pdf["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean()
    rs = avg_gain / avg_loss
    pdf["RSI"] = 100 - (100 / (1 + rs))

    return pdf


# Construir esquema de salida: columnas Silver + 4 KPIs
output_schema = (
    df_silver.select(*silver_columns)
    .schema.add("SMA_200", DoubleType())
    .add("EMA_50", DoubleType())
    .add("MACD", DoubleType())
    .add("RSI", DoubleType())
)

# Aplicar la función distribuida agrupando por símbolo
# (cada grupo tiene toda la serie temporal de un activo)
df_gold = (
    df_silver.select(*silver_columns)
    .groupBy("symbol")
    .applyInPandas(calcular_kpis, schema=output_schema)
)

print(f"[INFO] Registros con KPIs calculados: {df_gold.count()}")

# ==================================================
# 3. AÑADIR PARTICIONES Y ESCRITURA EN GOLD
# ==================================================
df_gold = df_gold.withColumn("year", F.year("datetime"))
df_gold = df_gold.withColumn("month", F.date_format("datetime", "MM"))

print(f"[INFO] Escribiendo Parquet en {GOLD_PATH}")

(df_gold.write.mode("overwrite").partitionBy("year", "month").parquet(GOLD_PATH))

print(f"[OK] Capa Gold generada en {GOLD_PATH}")

# ==================================================
# FIN
# ==================================================
job.commit()
print("[FIN] Job Silver → Gold completado.")
