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
TOPIC = "imat3a-DOT"
GROUP_ID = "imat3a_dot_console_group"

# --- Configuración Timestream ---
REGION = "eu-west-1"
DATABASE = "imat3a_crypto_rt"
QUOTES_TABLE = "dot_close"

ts_client = boto3.client("timestream-write", region_name=REGION)

def iso_to_epoch_ms(value: str) -> str:
    # Parsea el timestamp ISO 8601 a milisegundos epoch
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))

def parse_value(raw_value: bytes) -> dict:
    text = raw_value.decode("utf-8").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        brace_pos = text.find("{")
        if brace_pos == -1:
            raise
        return json.loads(text[brace_pos:])

def procesar_y_guardar_close(record_key: str, record_value: dict) -> None:
    try:
        symbol = (record_value.get("symbol") or record_key or "DOTUSDT").upper()
        close_value = float(record_value["close"])
        event_timestamp = record_value["@timestamp"]
        
        # Crear el registro para Timestream
        quote_record = {
            "Dimensions": [
                {"Name": "symbol", "Value": symbol},
                {"Name": "source_topic", "Value": TOPIC}
            ],
            "MeasureName": "close",
            "MeasureValue": str(close_value),
            "MeasureValueType": "DOUBLE",
            "Time": iso_to_epoch_ms(event_timestamp),
            "TimeUnit": "MILLISECONDS",
        }

        # Escribir en Timestream
        ts_client.write_records(
            DatabaseName=DATABASE,
            TableName=QUOTES_TABLE,
            Records=[quote_record],
        )
        print(f"[OK] {symbol} | close={close_value} guardado en {QUOTES_TABLE}.")

    except Exception as e:
        print(f"[ERROR] No se pudo guardar el mensaje: {e} | value={record_value}")

def parse_value(raw_value: bytes) -> dict:
    text = raw_value.decode("utf-8").strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Soporta líneas tipo: DOTUSDT {"symbol": ...}
        brace_pos = text.find("{")
        if brace_pos == -1:
            raise
        return json.loads(text[brace_pos:])


def mostrar_close_volume(record_key: str, record_value: dict) -> None:
    try:
        symbol = (record_value.get("symbol") or record_key or "DOTUSDT").upper()
        close_value = float(record_value["close"])
        volume_value = float(record_value["volume"])
        event_timestamp = record_value["@timestamp"]
        print(
            f"[MSG] {symbol} | close={close_value} | volume={volume_value} | ts={event_timestamp}"
        )

    except Exception as e:
        print(f"[ERROR] No se pudo mostrar el mensaje: {e} | value={record_value}")


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
        value_deserializer=parse_value,
    )

    consumer.assign([TopicPartition(TOPIC, 0)])
    print(f"[*] Escuchando topic '{TOPIC}' y mostrando close/volume por consola...\n")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for _, consumer_records in records.items():
                for record in consumer_records:
                    mostrar_close_volume(record.key, record.value)
                    procesar_y_guardar_close(record.key, record.value)

    except KeyboardInterrupt:
        print("\n[!] Deteniendo el consumidor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
