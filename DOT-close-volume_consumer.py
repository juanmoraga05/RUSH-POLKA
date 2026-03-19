# -*- coding: utf-8 -*-
import json

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# --- Configuración Kafka ---
BOOTSTRAP_SERVERS = "51.49.235.244:9092"
USERNAME = "kafka_client"
PASSWORD = "88b8a35dca1a04da57dc5f3e"
TOPIC = "imat3a-DOT"
GROUP_ID = "imat3a_dot_console_group"


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
    except KeyboardInterrupt:
        print("\n[!] Deteniendo el consumidor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
