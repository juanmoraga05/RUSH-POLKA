# -*- coding: utf-8 -*-
import json
import time
from datetime import datetime, timezone
import boto3
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# --- Configuración Kafka ---
BOOTSTRAP_SERVERS = "51.49.235.244:9092"
USERNAME = "kafka_client"
PASSWORD = "88b8a35dca1a04da57dc5f3e"
TOPIC = "imat3a-DOT-VWAP"
GROUP_ID = "imat3a_vwap_console_group"

# --- Configuración Timestream ---
REGION = "eu-west-1"
DATABASE = "imat3a_crypto_rt"
VWAP_TABLE = "dot_vwap"

ts_client = boto3.client("timestream-write", region_name=REGION)


def mostrar_vwap(record_key: str, record_value: dict):
    try:
        symbol = (record_value.get("symbol") or record_key or "DOTUSDT").upper()
        vwap_value = record_value.get("vwap")
        print(f"[MSG] {symbol} | vwap={vwap_value} | payload={record_value}")

    except Exception as e:
        print(f"[ERROR] No se pudo mostrar el mensaje: {e} | value={record_value}")

def now_epoch_ms() -> str:
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))

def procesar_y_guardar_vwap(record_key: str, record_value: dict):
    try:
        symbol = (record_value.get("symbol") or record_key or "DOTUSDT").upper()
        vwap_value = record_value.get("vwap")
        
        # Extraer timestamps si vienen en el payload, si no, usar el actual
        window_start = record_value.get("window_start", "N/A")
        window_end = record_value.get("window_end", "N/A")
        
        vwap_record = {
            "Dimensions": [
                {"Name": "symbol", "Value": symbol},
                {"Name": "source_topic", "Value": TOPIC},
                {"Name": "window_start", "Value": str(window_start)},
                {"Name": "window_end", "Value": str(window_end)}
            ],
            "MeasureName": "vwap",
            "MeasureValue": str(float(vwap_value)),
            "MeasureValueType": "DOUBLE",
            # Usamos el epoch actual ya que el payload simple a veces no trae el timestamp de evento exacto
            "Time": now_epoch_ms(), 
            "TimeUnit": "MILLISECONDS"
        }

        ts_client.write_records(
            DatabaseName=DATABASE,
            TableName=VWAP_TABLE,
            Records=[vwap_record],
        )
        print(f"[OK] {symbol} | vwap={vwap_value} guardado en {VWAP_TABLE}.")

    except Exception as e:
        print(f"[ERROR] No se pudo guardar el mensaje: {e} | value={record_value}")

def main() -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    consumer.assign([TopicPartition(TOPIC, 0)])
    print(f"[*] Escuchando el topic '{TOPIC}' y mostrando mensajes por consola...\n")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for _, consumer_records in records.items():
                for record in consumer_records:
                    mostrar_vwap(record.key, record.value)
                    procesar_y_guardar_vwap(record.key, record.value)

    except KeyboardInterrupt:
        print("\n[!] Deteniendo el consumidor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
