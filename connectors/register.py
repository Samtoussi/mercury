
import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = Path(__file__).with_name("postgres-source.example.json")
CONNECT_URL = "http://localhost:8083/connectors"

load_dotenv(ROOT / ".env")


def main():
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is missing from .env")

    with TEMPLATE.open(encoding="utf-8") as file:
        connector = json.load(file)

    connector["config"]["database.password"] = password

    name = connector["name"]
    url = f"{CONNECT_URL}/{name}/config"

    # PUT creates the connector if it does not exist,
    # or updates its configuration if it already exists.
    body = json.dumps(connector["config"]).encode("utf-8")

    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )

    try:
        with urlopen(request, timeout=15) as response:
            result = json.load(response)

    except HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Connector registration failed (HTTP {error.code}): {details}"
        ) from error

    except URLError as error:
        raise RuntimeError(
            "Cannot reach Kafka Connect at localhost:8083"
        ) from error

    print(f"Connector registered: {result['name']}")


if __name__ == "__main__":
    main()
