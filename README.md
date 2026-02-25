# Walgo

A persistent, crash-safe **key-value store** written in Go, built on top of a custom **Write-Ahead Log (WAL)** engine. This project was written from scratch to deeply understand how databases achieve durability — the WAL layer is fully hand-rolled with no external storage libraries.

---

## Overview

Most databases don't write to their real data structures first — they write to a log. If the process crashes, the log is replayed to reconstruct state. This project implements exactly that pattern: a generic WAL engine paired with a simple in-memory KV store that is durable across restarts.

```
┌──────────────────────────────────────┐
│              KV Store                │
│  GET / SET / DELETE / Recover        │
│  in-memory map[string]string         │
└──────────────┬───────────────────────┘
               │ every write
               ▼
┌──────────────────────────────────────┐
│         Write-Ahead Log (WAL)        │
│  segment-0 │ segment-1 │ segment-2   │
│  (binary encoded, CRC32 verified)    │
└──────────────────────────────────────┘
```

---

## Packages

### `wal` — Write-Ahead Log Engine

The core of the project. A fully self-contained WAL implementation with:

| Feature | Details |
|---|---|
| **Segmented log files** | Log is split into numbered segment files (`segment-0`, `segment-1`, …) |
| **Automatic log rotation** | When a segment hits the configured max size, it's flushed and a new segment is created |
| **Oldest segment eviction** | When `maxSegments` is reached, the oldest segment is deleted automatically |
| **Buffered writes** | Uses `bufio.Writer` for batched I/O — writes go to an in-memory buffer first |
| **Background sync** | A goroutine flushes the buffer to the kernel every **500ms** |
| **Optional fsync** | Configurable flag to call `file.Sync()` after every flush, forcing a disk write |
| **CRC32 integrity** | Every record is checksummed using `crc32.IEEE` over `(LSN + data)`. Corrupt records are rejected on read |
| **Log Sequence Numbers** | Every record gets a monotonically increasing LSN, persisted across restarts |
| **Graceful shutdown** | `Close()` cancels the background goroutine via `context`, flushes, and closes the file |

#### WAL Record binary format

Each record is stored as a length-prefixed binary blob:

```
[ 4 bytes: record size ] [ 8 bytes: LSN ] [ 4 bytes: CRC32 ] [ N bytes: data ]
```

#### Key WAL API

| Method | Description |
|---|---|
| `OpenWAL(dir, fsync, maxSize, maxSegments)` | Opens or resumes an existing WAL. Creates a new one if the directory is empty |
| `WriteEntry(data []byte)` | Appends a record to the buffered log |
| `Sync()` | Flushes the buffer to the kernel (and optionally to disk) |
| `ReadCurrentFileLogs()` | Reads all records from the active segment |
| `ReadAllLogsFromOffset(offset int)` | Reads all records from segment `offset` onwards — used for recovery |
| `Close()` | Flushes, syncs, and closes the WAL cleanly |

---

### `store` — Key-Value Store

A concurrent in-memory KV store that uses the WAL for durability.

| Feature | Details |
|---|---|
| **In-memory storage** | `map[string]string` guarded by `sync.RWMutex` |
| **Write-ahead logging** | Every `SET` and `DELETE` is logged to WAL before modifying the map |
| **Crash recovery** | `Recover()` replays all WAL segments from offset 0 to rebuild state |
| **Concurrent reads** | `RLock` lets multiple goroutines read simultaneously |
| **Serialized writes** | `Lock` ensures safe concurrent `SET`/`DELETE` |

#### Store configuration (hardcoded defaults)

| Parameter | Value |
|---|---|
| Max segment file size | 64 MB |
| Max segments retained | 5 |
| fsync | Disabled (buffer-only sync) |

#### Store API

| Method | Description |
|---|---|
| `InitializeStore(dir)` | Opens a store backed by a WAL in the given directory |
| `SET(key, value string)` | Writes to WAL then updates the in-memory map |
| `GET(key string)` | Reads directly from the in-memory map |
| `DELETE(key string)` | Writes a tombstone to WAL then deletes from the map |
| `Recover()` | Replays WAL from the beginning to restore in-memory state |

---

## Benchmarks

Benchmarks are written with Go's `testing` package and measure raw throughput.

### WAL benchmarks (`wal/`)

| Benchmark | Setup | What it measures |
|---|---|---|
| `BenchmarkWriteThroughPut` | 10M entries, single goroutine | Raw sequential write throughput |
| `BenchmarkReadThroughPut` | 10M entries pre-written | Read (replay) throughput from active segment |
| `BenchmarkConcurrentWriteThroughPut` | 100 goroutines × 10K entries | Concurrent write throughput under lock contention |

### Store benchmarks (`store/`)

| Benchmark | Setup | What it measures |
|---|---|---|
| `BenchmarkWriteThroughPut` | 10M entries | End-to-end `SET` throughput (WAL + map write) |
| `BenchmarkReadThroughPut` | 10M entries pre-loaded | End-to-end `GET` throughput from in-memory map |
| `BenchmarkConcurrentWriteThroughPut` | 1000 goroutines × 10K entries | High-concurrency `SET` throughput |

Run benchmarks:

```bash
# WAL benchmarks
make bench-wal

# Store benchmarks
make bench-store
```

### Results

**Store benchmarks** (`make bench-store`)

![Store benchmark results](assets/image1.png)

**WAL benchmarks** (`make bench-wal`)

![WAL benchmark results](assets/imag2.png)

---

## Tests

```bash
# WAL tests
make test-wal

# Store tests
make test-store
```

Tests cover WAL correctness (record integrity, rotation, recovery, CRC rejection), and store correctness (CRUD, recovery after simulated crash).

---

## Tech Stack

| | |
|---|---|
| Language | Go |
| Serialization | `encoding/binary` (little-endian) + `encoding/json` |
| Integrity | `hash/crc32` (IEEE polynomial) |
| JSON parsing | [`gjson`](https://github.com/tidwall/gjson) |
| Test runner | [`gotestsum`](https://github.com/gotestyourself/gotestsum) + [`testify`](https://github.com/stretchr/testify) |

