import os
import pandas as pd
import boto3
from botocore.exceptions import ClientError

from TradingviewData import TradingViewData, Interval

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


# ==================================================
# S3 HELPERS (boto3 usa variables de entorno)
# ==================================================
def ensure_bucket_exists(s3_client, bucket_name: str, region: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[OK] Bucket accesible: {bucket_name}")
        return
    except ClientError:
        pass

    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"[OK] Bucket creado: {bucket_name} ({region})")
    except ClientError as e:
        print(f"[ERROR] No se pudo crear el bucket: {e}")
        raise


def upload_file_to_s3(s3_client, bucket: str, local_path: str, s3_key: str) -> None:
    try:
        s3_client.upload_file(local_path, bucket, s3_key)
        print(f"[OK] Subido a S3: s3://{bucket}/{s3_key}")
    except ClientError as e:
        print(f"[ERROR] Fallo subiendo a S3: {e}")
        raise


# ==================================================
# UTILIDADES DE PATHS
# ==================================================
def build_s3_prefix(year: int, month: int) -> str:
    return (
        f"{BASE_PREFIX}/"
        f"exchange={EXCHANGE}/"
        f"symbol={SYMBOL}/"
        f"interval={INTERVAL}/"
        f"year={year}/"
        f"month={month:02d}/"
    )


def build_filename(year: int, month: int) -> str:
    return f"{SYMBOL}_{EXCHANGE}_{INTERVAL}_{year}-{month:02d}.csv"


# ==================================================
# CSV
# ==================================================
def write_csv(df: pd.DataFrame, year: int, month: int) -> str:
    if df.empty:
        return ""

    df["symbol"] = SYMBOL
    df["exchange"] = EXCHANGE
    df["interval"] = INTERVAL
    df["source"] = SOURCE

    cols = [
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "symbol",
        "exchange",
        "interval",
        "source",
    ]
    df = df[cols]

    prefix = build_s3_prefix(year, month)
    filename = build_filename(year, month)

    local_dir = os.path.join(LOCAL_BASE_DIR, prefix)
    os.makedirs(local_dir, exist_ok=True)

    local_path = os.path.join(local_dir, filename)
    df.to_csv(local_path, index=False, encoding="utf-8")

    print(f"[OK] CSV creado: {local_path} ({len(df)} filas)")
    return local_path


# ==================================================
# MAIN
# ==================================================
def main():
    print("[INFO] Usando credenciales AWS desde variables de entorno (PowerShell)")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    ensure_bucket_exists(s3, BUCKET_NAME, AWS_REGION)

    tv = TradingViewData()  # Inicialización para evitar retrasos en el bucle
    polka_data = tv.get_hist(
        symbol=SYMBOL, exchange=EXCHANGE, interval=Interval.daily, n_bars=1460
    )

    # Reset index to make 'datetime' a column, so it is preserved in CSV
    polka_data.reset_index(inplace=True)

    polka_data["year"] = polka_data["datetime"].dt.year
    polka_data["month"] = polka_data["datetime"].dt.month

    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            print(f"\nProcesando {year}-{month:02d}")

            df = polka_data[
                (polka_data["year"] == year) & (polka_data["month"] == month)
            ].copy()
            if df.empty:
                print("[INFO] No hay datos para este mes.")
                continue

            # aquí dejas solo las columnas que necesitas
            if "volume" not in df.columns:
                df["volume"] = pd.NA

            df = df[["datetime", "open", "high", "low", "close", "volume"]]

            local_csv = write_csv(df, year, month)
            if not local_csv:
                continue

            s3_key = build_s3_prefix(year, month) + os.path.basename(local_csv)
            upload_file_to_s3(s3, BUCKET_NAME, local_csv, s3_key)
    print("\n[FIN] HU-2 completada correctamente.")


if __name__ == "__main__":
    main()
