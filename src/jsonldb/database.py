import json
import struct
import uuid
from pathlib import Path
from typing import Any, Iterator

from .compactor import Compactor
from .index import Index
from .record import Record
from .wal import WAL, WalEntry


class Database:
    def __init__(self, path: str, index_fields: list[str] | None = None):
        self.path = Path(path)
        self.data_dir = self.path / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.wal_path = self.data_dir / "wal.jsonl"
        self.data_path = self.data_dir / "data.jsonl"
        self.index = Index(str(self.data_dir / "index.lmdb"))
        self._index_fields = index_fields or []

        self.wal = WAL(self.wal_path)
        self.compactor = Compactor(self.data_path, self.index, self.wal, self._index_fields)

    def insert(self, data: dict[str, Any]) -> str:
        record_id = str(uuid.uuid4())

        wal_entry = WalEntry(op="insert", record_id=record_id, data=data)
        self.wal.append(wal_entry)

        self._apply_to_data(record_id, data)

        return record_id

    def _apply_to_data(self, record_id: str, data: dict[str, Any]) -> None:
        record = Record(id=record_id, data=data)

        offset = 0
        if self.data_path.exists():
            offset = self.data_path.stat().st_size

        with open(self.data_path, "a") as f:
            line = record.to_jsonl() + "\n"
            f.write(line)
            end_offset = f.tell()

        self.index.put(record_id.encode(), struct.pack("QQ", offset, end_offset))

        for field in self._index_fields:
            if field in data:
                index_key = f"{field}:{json.dumps(data[field])}".encode()
                self.index.put_index(index_key, record_id.encode())

    def get(self, record_id: str) -> Record | None:
        offset_data = self.index.get(record_id.encode())
        if offset_data is None:
            return self._get_from_wal(record_id)

        offset, end_offset = struct.unpack("QQ", offset_data)

        with open(self.data_path, "r") as f:
            f.seek(offset)
            line = f.read(end_offset - offset).strip()
            return Record.from_jsonl(line)

    def _get_from_wal(self, record_id: str) -> Record | None:
        last_entry = None
        for entry in self.wal.scan():
            if entry.record_id == record_id:
                last_entry = entry

        if last_entry is None:
            return None

        if last_entry.op == "delete":
            return None
        if last_entry.op in ("insert", "update") and last_entry.data:
            return Record(id=record_id, data=last_entry.data)
        return None

    def query(self, field: str, value: Any) -> Iterator[Record]:
        index_key = f"{field}:{json.dumps(value)}".encode()
        record_ids = self.index.get_index(index_key)

        for record_id in record_ids:
            record = self.get(record_id.decode())
            if record:
                yield record

    def update(self, record_id: str, data: dict[str, Any]) -> bool:
        existing = self.get(record_id)
        if existing is None:
            return False

        new_data = {**existing.data, **data}

        wal_entry = WalEntry(op="update", record_id=record_id, data=new_data)
        self.wal.append(wal_entry)

        self._apply_to_data(record_id, new_data)

        return True

    def delete(self, record_id: str) -> bool:
        if self.get(record_id) is None:
            return False

        wal_entry = WalEntry(op="delete", record_id=record_id)
        self.wal.append(wal_entry)

        self.index.delete(record_id.encode())

        return True

    def all(self) -> Iterator[Record]:
        seen = set()

        for record in self._scan_data():
            seen.add(record.id)
            yield record

        for entry in self.wal.scan():
            if entry.record_id in seen:
                continue
            if entry.op == "delete":
                continue
            if entry.op in ("insert", "update") and entry.data:
                yield Record(id=entry.record_id, data=entry.data)

    def _scan_data(self) -> Iterator[Record]:
        if not self.data_path.exists():
            return
        with open(self.data_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield Record.from_jsonl(line)

    def count(self) -> int:
        return sum(1 for _ in self.all())

    def compact(self) -> int:
        return self.compactor.compact()
