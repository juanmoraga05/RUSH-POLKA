from botocore.exceptions import ClientError
from config import (
    SYMBOL,
    EXCHANGE,
    INTERVAL,
    SOURCE,
    BRONZE_LOCAL_DIR,
    BRONZE_S3_PREFIX,
)
import pandas as pd
import os


# ==================================================
# S3 UTILS (boto3 usa variables de entorno)
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
# PATH UTILS
# ==================================================
def build_s3_prefix(year: int, month: int, layer: str = BRONZE_S3_PREFIX) -> str:
    return f"{layer}/year={year}/month={month:02d}/"


def build_filename(year: int, month: int) -> str:
    return f"{SYMBOL}_{EXCHANGE}_{INTERVAL}_{year}-{month:02d}.csv"


# ==================================================
# CSV UTILS
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

    partition = f"year={year}/month={month:02d}"
    filename = build_filename(year, month)

    local_dir = os.path.join(BRONZE_LOCAL_DIR, partition)
    os.makedirs(local_dir, exist_ok=True)

    local_path = os.path.join(local_dir, filename)
    df.to_csv(local_path, index=False, encoding="utf-8")

    print(f"[OK] CSV (Bronze) creado: {local_path} ({len(df)} filas)")
    return local_path
