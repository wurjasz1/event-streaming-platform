import threading
import json
from typing import Any,Generator
import os

class LocalSpool:
    def __init__(self, filepath: str = "spool.jsonl"):
        self.filepath=filepath
        self.lock=threading.Lock()

    def append(self, record: dict[str,Any]) -> None:
        #write record to file
        with self.lock:
            with open(self.filepath, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

    def read_batches(self, batch_size: int = 100) -> Generator[list[dict[str,Any]], None, None]:
        #streaming read
        if not os.path.exists(self.filepath):
            return

        with self.lock:
            with open(self.filepath, "r", encoding="utf-8") as f:
                batch = []

                for line in f:
                    batch.append(json.loads(line))

                    if len(batch)>=batch_size:
                        yield batch
                        batch=[]

                if batch:
                    yield batch

    def clear(self) -> None:
        #clear spool after successful replay
        with self.lock:
            if os.path.exists(self.filepath):
                os.remove(self.filepath)