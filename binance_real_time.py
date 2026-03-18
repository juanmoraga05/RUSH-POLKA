# -*- coding: utf-8 -*-
import json
import os
import time
from datetime import datetime, timezone
from binance import Client
from binance import ThreadedWebsocketManager
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaConnectionError, NoBrokersAvailable

# ==================================================
# CONFIGURACIÓN KAFKA
# ==================================================
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "51.49.235.244:9092")
USERNAME = os.getenv("KAFKA_USERNAME", "kafka_client")
PASSWORD = os.getenv("KAFKA_PASSWORD", "88b8a35dca1a04da57dc5f3e")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "imat3a-DOT")
CREATE_TOPIC_IF_MISSING = os.getenv("KAFKA_CREATE_TOPIC", "true").lower() in {
    "1",
    "true",
    "yes",
    "y",
}

SYMBOL = os.getenv("BINANCE_SYMBOL", "DOTUSDT").upper()
INTERVAL_RAW = os.getenv("BINANCE_INTERVAL", "5m").lower()

INTERVAL_MAP = {
    "1m": Client.KLINE_INTERVAL_1MINUTE,
    "3m": Client.KLINE_INTERVAL_3MINUTE,
    "5m": Client.KLINE_INTERVAL_5MINUTE,
    "15m": Client.KLINE_INTERVAL_15MINUTE,
    "30m": Client.KLINE_INTERVAL_30MINUTE,
    "1h": Client.KLINE_INTERVAL_1HOUR,
    "2h": Client.KLINE_INTERVAL_2HOUR,
    "4h": Client.KLINE_INTERVAL_4HOUR,
    "6h": Client.KLINE_INTERVAL_6HOUR,
    "8h": Client.KLINE_INTERVAL_8HOUR,
    "12h": Client.KLINE_INTERVAL_12HOUR,
    "1d": Client.KLINE_INTERVAL_1DAY,
    "3d": Client.KLINE_INTERVAL_3DAY,
    "1w": Client.KLINE_INTERVAL_1WEEK,
    "1mo": Client.KLINE_INTERVAL_1MONTH,
}

INTERVAL = INTERVAL_MAP.get(INTERVAL_RAW, Client.KLINE_INTERVAL_5MINUTE)

# Fallback de versiones API para brokers que no responden a auto-discovery
KAFKA_API_VERSION_FALLBACKS = [
    (3, 0, 0),
    (2, 8, 0),
    (2, 0, 0),
    (0, 10, 2),
]


def _print_kafka_hint(error: Exception) -> None:
    msg = str(error)
    if "bootstrap-0" in msg:
        print(
            "[HINT] El broker está devolviendo un host interno ('bootstrap-0'). "
            "Esto suele ser un problema de advertised.listeners en el servidor Kafka."
        )
    print(
        "[HINT] Verifica con el admin del broker: listener externo, protocolo (SASL_PLAINTEXT/SASL_SSL), credenciales y ACL del topic."
    )


def _print_runtime_config() -> None:
    print("[INFO] Configuración activa:")
    print(f"       Kafka bootstrap: {BOOTSTRAP_SERVERS}")
    print(f"       Kafka protocol : {SECURITY_PROTOCOL}/{SASL_MECHANISM}")
    print(f"       Kafka topic    : {TOPIC_NAME}")
    print(f"       Create topic   : {CREATE_TOPIC_IF_MISSING}")
    print(f"       Binance symbol : {SYMBOL}")
    print(f"       Binance interval: {INTERVAL} (raw={INTERVAL_RAW})")


def _kafka_base_config() -> dict:
    return {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "security_protocol": SECURITY_PROTOCOL,
        "sasl_mechanism": SASL_MECHANISM,
        "sasl_plain_username": USERNAME,
        "sasl_plain_password": PASSWORD,
        "request_timeout_ms": 20000,
        "api_version_auto_timeout_ms": 15000,
    }


def create_kafka_topic(topic_name):
    """Crea el topic en Kafka si no existe."""
    if not CREATE_TOPIC_IF_MISSING:
        print("[INFO] KAFKA_CREATE_TOPIC=false, no se intentará crear el topic.")
        return

    base_config = _kafka_base_config()

    admin_client = None
    try:
        admin_client = KafkaAdminClient(**base_config)
    except (NoBrokersAvailable, KafkaConnectionError) as e:
        _print_kafka_hint(e)
        for api_version in KAFKA_API_VERSION_FALLBACKS:
            try:
                print(
                    f"[WARN] Auto API version falló. Reintentando AdminClient con api_version={api_version}..."
                )
                admin_client = KafkaAdminClient(api_version=api_version, **base_config)
                break
            except (NoBrokersAvailable, KafkaConnectionError) as retry_error:
                _print_kafka_hint(retry_error)
                continue
    except Exception as e:
        print(f"[ERROR] No se pudo inicializar AdminClient: {e}")
        return

    if admin_client is None:
        print(
            "[ERROR] No se pudo inicializar AdminClient con ninguna API version. Verifica protocolo SASL/credenciales/estado del broker."
        )
        return

    try:
        # Partición 1, Factor de Replicación 1
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            topic_list = [
                NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            ]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"[INFO] Topic '{topic_name}' creado correctamente.")
        else:
            print(f"[INFO] El topic '{topic_name}' ya existe.")

        admin_client.close()
    except Exception as e:
        print(f"[ERROR] No se pudo crear/verificar el topic: {e}")


def get_kafka_producer():
    """Inicializa y devuelve un KafkaProducer."""
    base_config = _kafka_base_config()
    serializer_config = {
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda v: v.encode("utf-8"),
    }

    try:
        return KafkaProducer(**base_config, **serializer_config)
    except (NoBrokersAvailable, KafkaConnectionError) as e:
        _print_kafka_hint(e)
        for api_version in KAFKA_API_VERSION_FALLBACKS:
            try:
                print(
                    f"[WARN] Auto API version falló. Reintentando Producer con api_version={api_version}..."
                )
                return KafkaProducer(
                    api_version=api_version, **base_config, **serializer_config
                )
            except (NoBrokersAvailable, KafkaConnectionError) as retry_error:
                _print_kafka_hint(retry_error)
                continue

    raise NoBrokersAvailable(
        "No se pudo conectar a Kafka con auto API version ni con fallbacks. "
        "Revisa KAFKA_SECURITY_PROTOCOL, credenciales y estado del broker."
    )


# Variable global para el producer
producer = None


def handle_kline(msg):
    global producer
    """Callback para procesar los mensajes de Binance."""
    # Extraemos los datos de la vela
    k = msg["k"]

    # Sólo publicamos si la vela está cerrada ('x': True)
    if k["x"]:
        try:
            # Extraer campos requeridos
            symbol = k["s"]
            close_price = float(k["c"])
            volume = float(k["v"])

            # Timestamp (obtenido de Binance)
            # k['T'] es el timestamp de cierre de la vela en ms.
            # Convertimos a formato ISO 8601 UTC sin zona horaria
            timestamp_ms = int(k["T"])
            dt_object = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
            # Formato ejemplo: 2026-03-09T11:21:00Z
            timestamp_str = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Construir el mensaje JSON
            payload = {
                "symbol": symbol,
                "@timestamp": timestamp_str,
                "close": close_price,
                "volume": volume,
            }

            # Clave del mensaje: Símbolo de la criptomoneda
            key = symbol

            # Enviar a Kafka
            if producer:
                producer.send(
                    topic=TOPIC_NAME,
                    key=key,
                    value=payload,
                    timestamp_ms=timestamp_ms,
                )
                producer.flush()
                print(f"[SENT] {payload}")
            else:
                print(f"[ERROR] Producer no inicializado. Payload perdido: {payload}")

        except Exception as e:
            print(f"[ERROR] Error al procesar/enviar mensaje: {e}")


if __name__ == "__main__":
    _print_runtime_config()

    # 1. Crear el topic (si es necesario)
    create_kafka_topic(TOPIC_NAME)

    # 2. Inicializar el productor globalmente
    print("[INFO] Conectando a Kafka...")
    try:
        producer = get_kafka_producer()
        print("[INFO] Productor Kafka conectado.")
    except Exception as e:
        print(f"[ERROR] Falló la conexión con Kafka: {e}")
        exit(1)

    # 3. Iniciar el WebSocket de Binance
    print(f"[INFO] Iniciando stream de Binance para {SYMBOL} ({INTERVAL})...")
    twm = ThreadedWebsocketManager()
    twm.start()

    twm.start_kline_socket(symbol=SYMBOL, interval=INTERVAL, callback=handle_kline)

    print("Presiona Ctrl+C para detener el script.")
    try:
        # Mantener el script corriendo
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Deteniendo script...")
        twm.stop()
        if producer:
            producer.close()
        print("[INFO] Script finalizado.")
