import time
import boto3
from config import AWS_REGION, BUCKET_NAME, SYMBOL

# ==================================================
# CONFIGURACIÓN
# ==================================================
DATABASE_NAME = "trade_data_imat3a04"
CRAWLER_NAME = "dot_history_crawler"  # Nombre con referencia a la cripto
TABLE_PREFIX = "dot_"  # Prefijo para tablas → dot_year_xxxx, etc.
ROLE_ARN = "arn:aws:iam::490004641586:role/Sprint2a04"

# Los datos están en s3://polkadot-rush-imat/year=YYYY/month=MM/
# (build_s3_prefix en utils.py NO incluye 'raw/' en la key)
S3_TARGET_PATH = f"s3://{BUCKET_NAME}/"

# ==================================================
# CLIENTE GLUE
# ==================================================
glue = boto3.client("glue", region_name=AWS_REGION)


# ==================================================
# 1. CREAR BASE DE DATOS
# ==================================================
def create_database():
    try:
        glue.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,
                "Description": f"Base de datos para datos históricos de {SYMBOL}",
            }
        )
        print(f"[OK] Base de datos '{DATABASE_NAME}' creada.")
    except glue.exceptions.AlreadyExistsException:
        print(f"[OK] Base de datos '{DATABASE_NAME}' ya existe.")


# ==================================================
# 2. CREAR O RECREAR CRAWLER
# ==================================================
def delete_crawler_if_exists():
    """Elimina el crawler si ya existe, para poder recrearlo con nueva config."""
    try:
        state = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]["State"]
        if state != "READY":
            print(f"[WAIT] Crawler en estado '{state}', esperando a que termine...")
            while state != "READY":
                time.sleep(10)
                state = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]["State"]
        glue.delete_crawler(Name=CRAWLER_NAME)
        print(f"[OK] Crawler anterior '{CRAWLER_NAME}' eliminado.")
    except glue.exceptions.EntityNotFoundException:
        pass  # No existía, nada que borrar


def create_crawler():
    glue.create_crawler(
        Name=CRAWLER_NAME,
        Role=ROLE_ARN,
        DatabaseName=DATABASE_NAME,
        TablePrefix=TABLE_PREFIX,  # Tablas se llamarán dot_*
        Description=f"Crawler para indexar datos históricos de {SYMBOL} (Polkadot)",
        Targets={
            "S3Targets": [
                {"Path": S3_TARGET_PATH},
            ]
        },
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
        RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
    )
    print(f"[OK] Crawler '{CRAWLER_NAME}' creado con TablePrefix='{TABLE_PREFIX}'.")


# ==================================================
# 3. EJECUTAR CRAWLER Y ESPERAR
# ==================================================
def run_crawler():
    glue.start_crawler(Name=CRAWLER_NAME)
    print(f"[OK] Crawler '{CRAWLER_NAME}' iniciado.")
    print("[WAIT] Esperando a que finalice...")

    while True:
        time.sleep(15)
        state = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]["State"]
        if state == "READY":
            break
        print(f"  Estado: {state}...")

    # Mostrar métricas del último crawl
    metrics = glue.get_crawler_metrics(CrawlerNameList=[CRAWLER_NAME])
    for m in metrics["CrawlerMetricsList"]:
        print(f"  Tablas creadas: {m.get('TablesCreated', 0)}")
        print(f"  Tablas actualizadas: {m.get('TablesUpdated', 0)}")


# ==================================================
# 4. LISTAR TABLAS CREADAS
# ==================================================
def list_tables():
    response = glue.get_tables(DatabaseName=DATABASE_NAME)
    tables = response.get("TableList", [])
    if not tables:
        print("[WARN] No se encontraron tablas en la base de datos.")
    else:
        print(f"\n[OK] Tablas en '{DATABASE_NAME}':")
        for t in tables:
            print(f"  - {t['Name']}  (ubicación: {t['StorageDescriptor']['Location']})")


# ==================================================
# MAIN
# ==================================================
def main():
    print(f"=== Crawler automático para {SYMBOL} (Polkadot) ===\n")

    create_database()
    delete_crawler_if_exists()
    create_crawler()
    run_crawler()
    list_tables()

    print("\n=== Proceso completado ===")


if __name__ == "__main__":
    main()

