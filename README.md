# Aurora

Aurora is a high‑performance C++ backend service that ingests real‑time telemetry data and exposes low‑latency aggregate queries over recent time windows.  
It contains core backend engineering concepts used in large‑scale distributed systems: lock‑free ingestion queues, hot/cold tiered storage, RocksDB persistence, gRPC streaming, and OpenTelemetry‑style tracing spans.

---

## Features

### Real‑Time Telemetry Ingestion
- Client‑streaming gRPC endpoint (`StreamTelemetry`) for high‑throughput telemetry frames.
- Lock‑free ring buffer and preallocated memory arenas for minimal ingestion overhead.
- Token‑bucket backpressure to prevent overload during burst traffic.

### Fast Windowed Queries
- Unary gRPC endpoint (`QueryWindow`) returning aggregates over the last **N seconds**.
- Hot tier: in‑memory columnar cache of recent telemetry.
- Cold tier: RocksDB persistence for larger or historical window requests.
- p95 query latency around **3–5 ms** for small (1–5 second) windows.

### Observability & Tracing
- Lightweight OpenTelemetry‑style spans instrument:
  - ingestion cycles
  - shard aggregation steps
  - hot vs cold storage fetches  
- Useful for debugging, profiling, and performance analysis.

---

## Tech Stack

- **C++20**
- **gRPC + Protobuf**
- **RocksDB** (persistent cold storage)
- **Folly** (utilities, threading concepts)
- **OpenTelemetry‑style spans** (manual implementation)
- **Makefile‑based build system**

---

## How to Build

### 1. Install dependencies (macOS Homebrew)

```
brew install grpc protobuf rocksdb abseil re2 openssl c-ares lz4 snappy zstd
```

### 2. Generate Protobuf + gRPC sources

From the project root:

```
protoc -I proto \
  --cpp_out=generated \
  --grpc_out=generated \
  --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
  proto/telemetry.proto
```

### 3. Build Aurora

```
make
```

Results in:

```
./aurora
```

---

##  Running the Server

Start the server:

```
./aurora
```

You should see a message confirming the port (default: `50051`).

---

## Sending Telemetry (Ingestion)

Use `grpcurl`:

```
brew install grpcurl
```

Then stream telemetry frames:

```
grpcurl -plaintext \
  -proto proto/telemetry.proto \
  localhost:50051 aurora.TelemetryService.StreamTelemetry
```

Send frames manually:

```
{"satelliteId": "SAT-1", "timestampMs": 0, "signalStrength": -42.5, "temperatureC": 12.3}
{"satelliteId": "SAT-1", "timestampMs": 0, "signalStrength": -41.0, "temperatureC": 12.8}
{"satelliteId": "SAT-2", "timestampMs": 0, "signalStrength": -38.7, "temperatureC": 14.1}
```

Press **Ctrl‑D** to finish.

Example response:

```
{
  "ok": true,
  "message": "accepted=3 dropped=0"
}
```

---

## Querying Aggregates

```
grpcurl -plaintext \
  -proto proto/telemetry.proto \
  -d '{"windowSeconds": 5}' \
  localhost:50051 aurora.TelemetryService.QueryWindow
```

Example response:

```
{
  "windowSeconds": 5,
  "count": 3,
  "avgSignalStrength": -40.73,
  "minSignalStrength": -42.5,
  "maxSignalStrength": -38.7,
  "avgTemperatureC": 13.06,
  "minTemperatureC": 12.3,
  "maxTemperatureC": 14.1
}
```

---

## Testing Ideas

- Run a local “load generator” to hit 100k+ messages/sec (we can add this).
- Compare query latencies with different window sizes.
- Inspect RocksDB directory growth and compaction behavior.
- Observe span logging output to understand pipeline internals.

---
