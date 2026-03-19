# -*- coding: utf-8 -*-
import json
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# --- Configuración Kafka ---
BOOTSTRAP_SERVERS = "51.49.235.244:9092"
USERNAME = "kafka_client"
PASSWORD = "88b8a35dca1a04da57dc5f3e"
TOPIC = "imat3a-DOT-VWAP"
GROUP_ID = "imat3a_vwap_console_group"


def mostrar_vwap(record_key: str, record_value: dict):
    try:
        symbol = (record_value.get("symbol") or record_key or "DOTUSDT").upper()
        vwap_value = record_value.get("vwap")
        print(f"[MSG] {symbol} | vwap={vwap_value} | payload={record_value}")

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
    except KeyboardInterrupt:
        print("\n[!] Deteniendo el consumidor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
