import os
import pandas as pd
import boto3
from TradingviewData import TradingViewData, Interval

from config import (
    AWS_REGION,
    BUCKET_NAME,
    SYMBOL,
    EXCHANGE,
    START_YEAR,
    END_YEAR,
    BRONZE_LOCAL_DIR,
    SILVER_LOCAL_DIR,
    GOLD_LOCAL_DIR,
)
from utils import ensure_bucket_exists, write_csv, build_s3_prefix, upload_file_to_s3



# ==================================================
# MAIN
# ==================================================
def main():
    print("[INFO] Usando credenciales AWS desde variables de entorno (PowerShell)")
    print("[INFO] Arquitectura Data Lake: Bronze → Silver → Gold")

    # Crear estructura de carpetas locales
    for layer_dir in (BRONZE_LOCAL_DIR, SILVER_LOCAL_DIR, GOLD_LOCAL_DIR):
        os.makedirs(layer_dir, exist_ok=True)
    print("[OK] Directorios de capas creados (bronze, silver, gold)")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    ensure_bucket_exists(s3, BUCKET_NAME, AWS_REGION)

    # Crear estructura de carpetas en S3 (bronze/, silver/, gold/)
    for layer in ("bronze", "silver", "gold"):
        s3.put_object(Bucket=BUCKET_NAME, Key=f"{layer}/", Body=b"")
    print("[OK] Estructura Data Lake creada en S3 (bronze/, silver/, gold/)")

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
    print("\n[FIN] Datos almacenados en capa Bronze correctamente.")


if __name__ == "__main__":
    main()
