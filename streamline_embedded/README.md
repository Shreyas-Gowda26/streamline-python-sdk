# Streamline Embedded (Python)

Run Streamline **in-process** — no Docker, no external processes. Perfect for testing.

## Install

```bash
pip install streamline-embedded
# Or build from source:
pip install maturin && maturin develop
```

## Usage

```python
from streamline_embedded import EmbeddedStreamline

# Context manager for automatic cleanup
with EmbeddedStreamline.in_memory() as sl:
    sl.create_topic("events", partitions=3)
    
    offset = sl.produce("events", partition=0, key=b"user-1", value=b'{"action":"click"}')
    
    records = sl.consume("events", partition=0, offset=0, max_records=10)
    for r in records:
        print(f"offset={r.offset} value={r.value}")
```

## vs Testcontainers

| | Embedded | Testcontainers |
|---|---|---|
| Startup time | **~1ms** | ~1s |
| Docker required | **No** | Yes |
| Persistence | Optional | Volume mount |
| Resource usage | **~5MB** | ~50MB |

## Building from Source

Requires Rust 1.80+ and maturin:

```bash
pip install maturin
cd streamline_embedded
maturin develop --release
```
