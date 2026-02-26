import json
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class Record:
    id: str
    data: dict[str, Any]

    def to_jsonl(self) -> str:
        obj = {"_id": self.id, **self.data}
        return json.dumps(obj)

    @classmethod
    def from_jsonl(cls, line: str) -> "Record":
        obj = json.loads(line)
        record_id = obj.pop("_id")
        return cls(id=record_id, data=obj)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
