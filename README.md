# JSONLDB

A NoSQL database built on JSONL format with LMDB indexing and Write-Ahead Log (WAL) for durability.

## Features

- **JSONL Storage** - Records stored as JSON Lines
- **LMDB Indexing** - Fast key-value index for lookups
- **WAL Architecture** - Write-Ahead Log ensures durability
- **Compaction** - Periodic compaction to merge WAL into data file
- **Secondary Indexes** - Index specific fields for fast queries

## Installation

```bash
pip install jsonldb
```

Or install from source:

```bash
pip install -e .
```

## Quick Start

```python
from jsonldb import Database

# Create database with indexed fields
db = Database("/path/to/db", index_fields=["email", "age"])

# Insert a record
id = db.insert({"name": "Alice", "email": "alice@example.com", "age": 30})

# Get by ID
record = db.get(id)
print(record.data)  # {'name': 'Alice', 'email': 'alice@example.com', 'age': 30}

# Query by indexed field
for r in db.query("age", 30):
    print(r.data)

# Update
db.update(id, {"age": 31})

# Delete
db.delete(id)

# Compact - rebuild data file and clear WAL
db.compact()
```

## Architecture

```
data/
├── wal.jsonl    # Write-Ahead Log (all operations)
├── data.jsonl   # Compacted data records
└── index.lmdb  # LMDB index → data.jsonl offsets
```

### Write Path
1. Append operation to WAL
2. Apply to data.jsonl
3. Update index

### Read Path
1. Check index → if found, read from data.jsonl
2. If not found, scan WAL for uncompacted records

### Compaction
- Merges data.jsonl + WAL into clean state
- Rebuilds index
- Clears WAL

## API

### Database

```python
db = Database(path, index_fields=None)
```

- `path`: Directory for database files
- `index_fields`: List of fields to index

#### Methods

| Method | Description |
|--------|-------------|
| `insert(data)` | Insert a record, returns ID |
| `get(id)` | Get record by ID |
| `query(field, value)` | Query by indexed field |
| `update(id, data)` | Update a record |
| `delete(id)` | Delete a record |
| `all()` | Iterate all records |
| `count()` | Count all records |
| `compact()` | Compact WAL into data file |

### Record

```python
record.id      # Record ID
record.data    # Record data as dict
record.to_dict()  # Convert to dict
```

## License

MIT
