"""
AWS Glue Job: Bronze → Silver
Lee los datos de la capa Bronze desde el Data Catalog (Glue),
limpia y tipifica los datos, y los almacena en formato Parquet en Silver.

Uso: subir este script a S3 y crear un Glue Job que lo ejecute.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

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
DATABASE_NAME = "trade_data_imat3a04"
BRONZE_TABLE = "dot_bronze"
SILVER_PATH = f"s3://{BUCKET}/silver/"

# ==================================================
# 1. LECTURA — Desde el Data Catalog (tabla Bronze)
# ==================================================
print(f"[INFO] Leyendo desde Data Catalog: {DATABASE_NAME}.{BRONZE_TABLE}")

dyf_bronze = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=BRONZE_TABLE,
)
df_bronze = dyf_bronze.toDF()

print(f"[INFO] Registros leídos desde Bronze: {df_bronze.count()}")
df_bronze.printSchema()

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
df_silver = df_silver.withColumn("month", F.date_format("datetime", "MM"))

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
