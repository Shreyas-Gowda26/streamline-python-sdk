//! Python bindings for Embedded Streamline.
//!
//! Provides a Python-native API for running Streamline in-process,
//! without Docker or external processes. Ideal for testing.
//!
//! ## Usage (Python)
//!
//! ```python
//! from streamline_embedded import EmbeddedStreamline
//!
//! # Start an in-memory instance
//! sl = EmbeddedStreamline.in_memory()
//!
//! # Create a topic
//! sl.create_topic("events", partitions=3)
//!
//! # Produce and consume
//! sl.produce("events", partition=0, key=b"k1", value=b"hello")
//! records = sl.consume("events", partition=0, offset=0, max_records=10)
//! for r in records:
//!     print(f"offset={r.offset} value={r.value}")
//!
//! sl.close()
//! ```

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;

/// A record returned by consume operations.
#[pyclass]
#[derive(Clone)]
struct Record {
    #[pyo3(get)]
    offset: i64,
    #[pyo3(get)]
    timestamp: i64,
    #[pyo3(get)]
    key: Option<Vec<u8>>,
    #[pyo3(get)]
    value: Vec<u8>,
}

#[pymethods]
impl Record {
    fn __repr__(&self) -> String {
        format!(
            "Record(offset={}, key={:?}, value={} bytes)",
            self.offset,
            self.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
            self.value.len()
        )
    }
}

/// Embedded Streamline instance — runs in-process, no Docker needed.
///
/// Use `EmbeddedStreamline.in_memory()` for ephemeral testing or
/// `EmbeddedStreamline(data_dir="/path")` for persistent storage.
#[pyclass]
struct EmbeddedStreamline {
    // In production, this holds a *mut StreamlineHandle from the C FFI.
    // For the scaffold, we use a placeholder that documents the API shape.
    _data_dir: Option<String>,
    _closed: bool,
}

#[pymethods]
impl EmbeddedStreamline {
    /// Create an embedded instance with persistent storage.
    #[new]
    #[pyo3(signature = (data_dir=None, default_partitions=1))]
    fn new(data_dir: Option<String>, default_partitions: i32) -> PyResult<Self> {
        let _ = default_partitions; // Used when FFI is linked
        Ok(Self {
            _data_dir: data_dir,
            _closed: false,
        })
    }

    /// Create an in-memory instance (no persistence, fastest for tests).
    #[staticmethod]
    fn in_memory() -> PyResult<Self> {
        Ok(Self {
            _data_dir: None,
            _closed: false,
        })
    }

    /// Create a topic with the given number of partitions.
    fn create_topic(&self, name: &str, partitions: Option<i32>) -> PyResult<()> {
        let _p = partitions.unwrap_or(1);
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        // TODO: Call streamline_create_topic via FFI
        let _ = name;
        Ok(())
    }

    /// Delete a topic.
    fn delete_topic(&self, name: &str) -> PyResult<()> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = name;
        Ok(())
    }

    /// Produce a record to a topic partition.
    fn produce(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> PyResult<i64> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = (topic, partition, key, value);
        // TODO: Call streamline_produce via FFI; return offset
        Ok(0)
    }

    /// Consume records from a topic partition.
    fn consume(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: Option<i32>,
    ) -> PyResult<Vec<Record>> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = (topic, partition, offset, max_records);
        // TODO: Call streamline_consume via FFI; return records
        Ok(vec![])
    }

    /// List all topics.
    fn list_topics(&self) -> PyResult<Vec<String>> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        Ok(vec![])
    }

    /// Get the latest offset for a partition.
    fn latest_offset(&self, topic: &str, partition: i32) -> PyResult<i64> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        let _ = (topic, partition);
        Ok(0)
    }

    /// Flush all pending writes.
    fn flush(&self) -> PyResult<()> {
        if self._closed {
            return Err(PyRuntimeError::new_err("Instance is closed"));
        }
        Ok(())
    }

    /// Close the instance and free resources.
    fn close(&mut self) -> PyResult<()> {
        self._closed = true;
        Ok(())
    }

    /// Get the Streamline version.
    #[staticmethod]
    fn version() -> &'static str {
        "0.2.0"
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

/// Streamline Embedded Python Module
#[pymodule]
fn streamline_embedded(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<EmbeddedStreamline>()?;
    m.add_class::<Record>()?;
    Ok(())
}
