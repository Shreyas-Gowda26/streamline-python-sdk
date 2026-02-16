"""Shared fixtures for Streamline SDK benchmarks."""

import pytest

from streamline_sdk.client import ClientConfig, ConsumerConfig, ProducerConfig
from streamline_sdk.retry import RetryConfig


@pytest.fixture
def default_client_config():
    """Pre-built default ClientConfig."""
    return ClientConfig()


@pytest.fixture
def custom_client_config():
    """Pre-built ClientConfig with non-default values."""
    return ClientConfig(
        bootstrap_servers="broker1:9092,broker2:9092,broker3:9092",
        client_id="bench-client",
        request_timeout_ms=60000,
        metadata_max_age_ms=600000,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username="bench-user",
        sasl_password="bench-pass",
    )


@pytest.fixture
def default_producer_config():
    """Pre-built default ProducerConfig."""
    return ProducerConfig()


@pytest.fixture
def default_consumer_config():
    """Pre-built default ConsumerConfig."""
    return ConsumerConfig()


@pytest.fixture
def default_retry_config():
    """Pre-built default RetryConfig."""
    return RetryConfig()


@pytest.fixture
def sample_headers():
    """Sample message headers for benchmarks."""
    return {
        "trace-id": b"abc-123-def-456",
        "content-type": b"application/json",
        "source": b"benchmark-suite",
    }
