# Testcontainers Streamline (Python)

[![PyPI](https://img.shields.io/pypi/v/testcontainers-streamline?style=flat-square)](https://pypi.org/project/testcontainers-streamline/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](LICENSE)

Testcontainers module for [Streamline](https://github.com/streamlinelabs/streamline) — **5x faster** than Kafka containers (~1s vs ~15s startup).

## Features

- Kafka-compatible container for testing
- Fast startup (~100ms vs seconds for Kafka)
- Low memory footprint (<50MB)
- No ZooKeeper or KRaft required
- Built-in health checks

## Installation

```bash
pip install testcontainers-streamline
```

## Usage

### Basic Usage

```python
from streamline_testcontainers import StreamlineContainer
from kafka import KafkaProducer, KafkaConsumer

# Using context manager (recommended)
with StreamlineContainer() as streamline:
    bootstrap_servers = streamline.get_bootstrap_servers()

    # Use with any Kafka client
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send("my-topic", b"Hello, Streamline!")
    producer.flush()
    producer.close()
```

### With pytest

```python
import pytest
from streamline_testcontainers import StreamlineContainer

@pytest.fixture(scope="module")
def streamline():
    with StreamlineContainer() as container:
        yield container

def test_kafka_integration(streamline):
    bootstrap_servers = streamline.get_bootstrap_servers()
    # Your test code here
```

### With Debug Logging

```python
with StreamlineContainer().with_debug_logging() as streamline:
    # Container will output debug logs
    pass
```

### Create Topics

```python
with StreamlineContainer() as streamline:
    streamline.create_topic("my-topic", partitions=3)
```

### Access HTTP API

```python
import requests

with StreamlineContainer() as streamline:
    # Health check
    response = requests.get(streamline.get_health_url())
    assert response.status_code == 200

    # Metrics
    metrics = requests.get(streamline.get_metrics_url()).text
```

### Custom Image Version

```python
with StreamlineContainer("streamline/streamline:0.2.0") as streamline:
    pass
```

## API Reference

### StreamlineContainer

| Method | Description |
|--------|-------------|
| `get_bootstrap_servers()` | Returns Kafka bootstrap servers string |
| `get_http_url()` | Returns HTTP API base URL |
| `get_health_url()` | Returns health check endpoint URL |
| `get_metrics_url()` | Returns Prometheus metrics URL |
| `create_topic(name, partitions)` | Creates a topic |
| `with_debug_logging()` | Enables debug logging |
| `with_trace_logging()` | Enables trace logging |

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

## License

Apache-2.0
