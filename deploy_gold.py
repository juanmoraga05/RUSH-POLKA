"""
Despliega y ejecuta el Glue Job Silver → Gold.
1. Ejecuta el Crawler para indexar Silver en el Data Catalog
2. Sube el script PySpark a S3
3. Crea (o actualiza) el Glue Job
4. Lo ejecuta y espera a que termine
"""

import os
import time
import boto3
from config import AWS_REGION, BUCKET_NAME, DATABASE_NAME, CRAWLER_NAME, ROLE_ARN
from crawler import (
    create_database,
    delete_crawler_if_exists,
    create_crawler,
    run_crawler,
    list_tables,
)

# ==================================================
# CONFIGURACIÓN
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOB_NAME = "dot_silver_to_gold"
SCRIPT_LOCAL = os.path.join(BASE_DIR, "glue_silver_to_gold.py")
SCRIPT_S3_KEY = "scripts/glue_silver_to_gold.py"
SCRIPT_S3_PATH = f"s3://{BUCKET_NAME}/{SCRIPT_S3_KEY}"

glue = boto3.client("glue", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


# ==================================================
# 1. SUBIR SCRIPT A S3
# ==================================================
def upload_script():
    s3.upload_file(SCRIPT_LOCAL, BUCKET_NAME, SCRIPT_S3_KEY)
    print(f"[OK] Script subido a {SCRIPT_S3_PATH}")


# ==================================================
# 2. CREAR O ACTUALIZAR GLUE JOB
# ==================================================
def create_or_update_job():
    job_config = dict(
        Name=JOB_NAME,
        Role=ROLE_ARN,
        Command={
            "Name": "glueetl",
            "ScriptLocation": SCRIPT_S3_PATH,
            "PythonVersion": "3",
        },
        DefaultArguments={
            "--job-language": "python",
            "--TempDir": f"s3://{BUCKET_NAME}/tmp/",
        },
        GlueVersion="4.0",
        WorkerType="G.1X",
        NumberOfWorkers=2,
        Timeout=30,  # minutos
        Description="Silver → Gold: Cálculo de indicadores técnicos (SMA, EMA, RSI, MACD)",
    )

    try:
        glue.get_job(JobName=JOB_NAME)
        # Ya existe → actualizar
        glue.update_job(
            JobName=JOB_NAME,
            JobUpdate={k: v for k, v in job_config.items() if k != "Name"},
        )
        print(f"[OK] Job '{JOB_NAME}' actualizado.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(**job_config)
        print(f"[OK] Job '{JOB_NAME}' creado.")


# ==================================================
# 3. EJECUTAR Y ESPERAR
# ==================================================
def run_job():
    response = glue.start_job_run(JobName=JOB_NAME)
    run_id = response["JobRunId"]
    print(f"[OK] Job iniciado (RunId: {run_id})")
    print("[WAIT] Esperando a que finalice...")

    while True:
        time.sleep(15)
        status = glue.get_job_run(JobName=JOB_NAME, RunId=run_id)["JobRun"]
        state = status["JobRunState"]

        if state in ("SUCCEEDED",):
            print("[OK] Job completado con éxito.")
            break
        elif state in ("FAILED", "TIMEOUT", "ERROR", "STOPPED"):
            error = status.get("ErrorMessage", "Sin mensaje de error")
            print(f"[ERROR] Job falló: {state} — {error}")
            break
        else:
            print(f"  Estado: {state}...")


# ==================================================
# MAIN
# ==================================================
def main():
    print("=== Deploy Glue Job: Silver → Gold ===\n")

    # Paso 1: Crawler para indexar Silver en el Data Catalog
    print("--- Paso 1: Ejecutar Crawler (indexar Silver) ---")
    create_database()
    delete_crawler_if_exists()
    create_crawler()
    run_crawler()
    list_tables()

    # Paso 2: Deploy y ejecución del Job
    print("\n--- Paso 2: Deploy y ejecución del Job ---")
    upload_script()
    create_or_update_job()
    run_job()
    print("\n=== Proceso completado ===")


if __name__ == "__main__":
    main()
