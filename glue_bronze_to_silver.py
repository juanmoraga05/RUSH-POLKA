"""
AWS Glue Job: Bronze → Silver
Lee los CSV particionados de la capa Bronze, limpia y tipifica los datos,
y los almacena en formato Parquet en la capa Silver.

Uso: subir este script a S3 y crear un Glue Job que lo ejecute.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

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
BRONZE_PATH = f"s3://{BUCKET}/bronze/"
SILVER_PATH = f"s3://{BUCKET}/silver/"

# ==================================================
# 1. LECTURA — CSV desde Bronze (con particiones year/month)
# ==================================================
print(f"[INFO] Leyendo CSVs desde {BRONZE_PATH}")

schema = StructType(
    [
        StructField("datetime", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("symbol", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("source", StringType(), True),
    ]
)

df_bronze = (
    spark.read.option("header", "true")
    .option("inferSchema", "false")
    .schema(schema)
    .csv(BRONZE_PATH)
)

print(f"[INFO] Registros leídos desde Bronze: {df_bronze.count()}")

# ==================================================
# 2. TRANSFORMACIÓN — Limpieza y tipificación (Silver)
# ==================================================

# Convertir datetime string → timestamp
df_silver = df_bronze.withColumn("datetime", F.to_timestamp("datetime"))

# Eliminar filas con datetime nulo (datos corruptos)
df_silver = df_silver.filter(F.col("datetime").isNotNull())

# Eliminar duplicados por datetime + symbol
df_silver = df_silver.dropDuplicates(["datetime", "symbol"])

# Ordenar cronológicamente
df_silver = df_silver.orderBy("datetime")

# Rellenar volumen nulo con 0
df_silver = df_silver.withColumn(
    "volume", F.when(F.col("volume").isNull(), 0.0).otherwise(F.col("volume"))
)

# Añadir columnas de partición (year/month) desde el datetime
df_silver = df_silver.withColumn("year", F.year("datetime"))
df_silver = df_silver.withColumn("month", F.month("datetime"))

print(f"[INFO] Registros tras limpieza (Silver): {df_silver.count()}")
df_silver.printSchema()

# ==================================================
# 3. ESCRITURA — Parquet particionado en Silver
# ==================================================
print(f"[INFO] Escribiendo Parquet en {SILVER_PATH}")

(df_silver.write.mode("overwrite").partitionBy("year", "month").parquet(SILVER_PATH))

print(f"[OK] Capa Silver generada en {SILVER_PATH}")

# ==================================================
# FIN
# ==================================================
job.commit()
print("[FIN] Job Bronze → Silver completado.")
