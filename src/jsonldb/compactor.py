import json
import struct
from pathlib import Path
from typing import Iterator

from .index import Index
from .record import Record
from .wal import WAL


class Compactor:
    def __init__(
        self, data_path: Path, index: Index, wal: WAL, index_fields: list[str]
    ):
        self.data_path = data_path
        self.index = index
        self.wal = wal
        self.index_fields = index_fields

    def compact(self) -> int:
        compacted_records = self._merge_records()

        offset_map = self._write_compacted_records(compacted_records)

        self._rebuild_indexes(offset_map)

        self.wal.clear()

        return len(offset_map)

    def _merge_records(self) -> dict[str, Record]:
        merged = {}

        for record in self._scan_data():
            merged[record.id] = record

        for entry in self.wal.scan():
            if entry.op == "delete":
                merged.pop(entry.record_id, None)
            elif entry.op in ("insert", "update") and entry.data:
                merged[entry.record_id] = Record(id=entry.record_id, data=entry.data)

        return merged

    def _scan_data(self) -> Iterator[Record]:
        if not self.data_path.exists():
            return
        with open(self.data_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield Record.from_jsonl(line)

    def _write_compacted_records(
        self, records: dict[str, Record]
    ) -> dict[str, tuple[int, int]]:
        new_data_path = self.data_path.with_suffix(".jsonl.tmp")
        offset_map = {}

        with open(new_data_path, "w") as f:
            offset = 0
            for record_id, record in records.items():
                line = record.to_jsonl() + "\n"
                f.write(line)
                end_offset = f.tell()
                offset_map[record_id] = (offset, end_offset)
                offset = end_offset

        if self.data_path.exists():
            self.data_path.unlink()
        new_data_path.rename(self.data_path)

        return offset_map

    def _rebuild_indexes(self, offset_map: dict[str, tuple[int, int]]) -> None:
        self.index.clear()

        for record_id, (offset, end_offset) in offset_map.items():
            self.index.put(record_id.encode(), struct.pack("QQ", offset, end_offset))

            with open(self.data_path, "r") as f:
                f.seek(offset)
                line = f.read(end_offset - offset).strip()
                record = Record.from_jsonl(line)

            for field in self.index_fields:
                if field in record.data:
                    index_key = f"{field}:{json.dumps(record.data[field])}".encode()
                    self.index.put_index(index_key, record_id.encode())
