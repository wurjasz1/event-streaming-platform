import json
from pathlib import Path
import requests

SCHEMA_REGISTRY_URL="http://localhost:8081"
SUBJECT_NAME="watch_event-value"
BASE_DIR=Path(__file__).resolve().parent.parent
SCHEMA_PATH=BASE_DIR/"schemas"/"watch_event_v1.json"

def main():
    schema_dict=json.loads(SCHEMA_PATH.read_text())
    payload={
        "schemaType": "JSON",
        "schema": json.dumps(schema_dict)
    }

    response=requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT_NAME}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json=payload,
        timeout=10
    )

    print("status:",response.status_code)
    print("response:",response.text)
    response.raise_for_status()

if __name__=="__main__":
    main()