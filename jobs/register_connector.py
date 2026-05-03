import os
import time
import requests


CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "postgres-connector")


CONFIG = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",

    "database.hostname": os.getenv("PG_HOST", "postgres"),
    "database.port": os.getenv("PG_PORT", "5432"),
    "database.user": os.getenv("PG_USER", "cdc_user"),
    "database.password": os.getenv("PG_PASSWORD", "cdc_pass"),
    "database.dbname": os.getenv("PG_DB", "sourcedb"),

    "topic.prefix": "dbserver1",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",

    "table.include.list": "public.customers,public.drivers",
    "snapshot.mode": "initial",

    "include.schema.changes": "false",
    "tombstones.on.delete": "true",
    "decimal.handling.mode": "double",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
}


def wait_for_connect(timeout_seconds=120):
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            response = requests.get(f"{CONNECT_URL}/connectors", timeout=10)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass

        time.sleep(5)

    raise RuntimeError("Kafka Connect did not become available in time.")


def upsert_connector():
    response = requests.put(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config",
        json=CONFIG,
        timeout=30,
    )

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to create/update connector. "
            f"Status={response.status_code}, body={response.text}"
        )

    print(f"Connector {CONNECTOR_NAME} created/updated successfully.")


def print_status():
    response = requests.get(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status",
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Could not read connector status. "
            f"Status={response.status_code}, body={response.text}"
        )

    print(response.text)


def main():
    wait_for_connect()
    upsert_connector()
    print_status()


if __name__ == "__main__":
    main()