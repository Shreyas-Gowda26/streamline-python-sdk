"""
Testcontainers module for Streamline - The Redis of Streaming.

Streamline is a Kafka-compatible streaming platform that provides a lightweight,
single-binary alternative to Apache Kafka for development and testing.

Quick Start:
    >>> from streamline_testcontainers import StreamlineContainer
    >>> with StreamlineContainer() as streamline:
    ...     bootstrap_servers = streamline.get_bootstrap_servers()
    ...     # Use with any Kafka client

Features:
    - Kafka protocol compatible - use existing Kafka clients unchanged
    - Fast startup (~100ms vs seconds for Kafka)
    - Low memory footprint (<50MB)
    - No ZooKeeper or KRaft required
    - Built-in HTTP API for health checks and metrics
"""

from .container import StreamlineContainer

__all__ = ["StreamlineContainer"]
__version__ = "0.2.0"
