import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator


@dataclass
class WalEntry:
    op: str
    record_id: str
    data: dict[str, Any] | None = None
    timestamp: float = field(default_factory=time.time)

    def to_jsonl(self) -> str:
        obj = {
            "op": self.op,
            "id": self.record_id,
            "ts": self.timestamp,
        }
        if self.data is not None:
            obj["data"] = self.data
        return json.dumps(obj)

    @classmethod
    def from_jsonl(cls, line: str) -> "WalEntry":
        obj = json.loads(line)
        return cls(
            op=obj["op"],
            record_id=obj["id"],
            data=obj.get("data"),
            timestamp=obj.get("ts", time.time()),
        )


class WAL:
    def __init__(self, path: Path):
        self.path = path

    def append(self, entry: WalEntry) -> None:
        with open(self.path, "a") as f:
            f.write(entry.to_jsonl() + "\n")

    def scan(self) -> Iterator[WalEntry]:
        if not self.path.exists():
            return
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield WalEntry.from_jsonl(line)

    def read_all(self) -> list[WalEntry]:
        return list(self.scan())

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()

    def count(self) -> int:
        if not self.path.exists():
            return 0
        with open(self.path, "r") as f:
            return sum(1 for line in f if line.strip())
