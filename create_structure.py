"""Crea la estructura de carpetas bronze/silver/gold en el bucket S3."""

import boto3
from config import AWS_REGION, BUCKET_NAME

s3 = boto3.client("s3", region_name=AWS_REGION)

for layer in ("bronze", "silver", "gold"):
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{layer}/", Body=b"")
    print(f"[OK] Creada: s3://{BUCKET_NAME}/{layer}/")

print("\n[FIN] Estructura del Data Lake creada.")
