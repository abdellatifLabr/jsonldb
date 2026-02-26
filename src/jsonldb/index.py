import lmdb


class Index:
    def __init__(self, path: str, map_size: int = 10 * 1024 * 1024):
        self.env = lmdb.open(path, map_size=map_size)

    def put(self, key: bytes, value: bytes) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(key, value)

    def get(self, key: bytes) -> bytes | None:
        with self.env.begin() as txn:
            return txn.get(key)

    def delete(self, key: bytes) -> bool:
        with self.env.begin(write=True) as txn:
            return txn.delete(key)

    def put_index(self, index_key: bytes, record_id: bytes) -> None:
        with self.env.begin(write=True) as txn:
            existing = txn.get(index_key)
            if existing:
                record_ids = existing.split(b",")
                if record_id not in record_ids:
                    record_ids.append(record_id)
                    txn.put(index_key, b",".join(record_ids))
            else:
                txn.put(index_key, record_id)

    def get_index(self, index_key: bytes) -> list[bytes]:
        with self.env.begin() as txn:
            existing = txn.get(index_key)
            if existing:
                return existing.split(b",")
            return []

    def count(self) -> int:
        with self.env.begin() as txn:
            return txn.stat()["entries"]

    def close(self) -> None:
        self.env.close()

    def clear(self) -> None:
        with self.env.begin(write=True) as txn:
            with txn.cursor() as cursor:
                for key, _ in cursor:
                    txn.delete(key)

    def remove_index_key(self, index_key: bytes, record_id: bytes) -> None:
        with self.env.begin(write=True) as txn:
            existing = txn.get(index_key)
            if existing:
                record_ids = existing.split(b",")
                if record_id in record_ids:
                    record_ids.remove(record_id)
                    if record_ids:
                        txn.put(index_key, b",".join(record_ids))
                    else:
                        txn.delete(index_key)
