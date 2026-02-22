# Changelog

All notable changes to this project will be documented in this file.
- test: add unit tests for producer batching (2026-02-22)
- refactor: improve consumer group coordinator logic (2026-02-22)
- fix: resolve asyncio event loop cleanup on shutdown (2026-02-22)
- fix: resolve event loop handling in producer close (2026-02-20)
- perf: optimize message batching with memoryview (2026-02-20)

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-02-18

### Added
- `StreamlineClient` with async context manager support
- `Producer` with async message sending and batching
- `Consumer` with async iteration and consumer group support
- `Admin` client for topic and group management
- Configuration via `StreamlineConfig` dataclass
- 8-type exception hierarchy with retryability flags
- Retry utilities with exponential backoff
- SASL authentication support (PLAIN, SCRAM)
- TLS/SSL connection support
- Testcontainers integration for testing

### Infrastructure
- CI pipeline with pytest, coverage reporting, and multi-Python matrix (3.9-3.12)
- CodeQL security scanning
- Release workflow with PyPI publishing
- Release drafter for automated release notes
- Dependabot for dependency updates
- CONTRIBUTING.md with development setup guide
- Security policy (SECURITY.md)
- EditorConfig for consistent formatting
- Ruff linter and formatter configuration
- MyPy type checking configuration
- Issue templates for bug reports and feature requests

## [0.1.0] - 2026-02-18

### Added
- Initial release of Streamline Python SDK
- Async-first design built on aiokafka
- Testcontainers support for integration testing
- Apache 2.0 license
