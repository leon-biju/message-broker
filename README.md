# Message Broker

A high-throughput, low-latency publish/subscribe message broker.

---

## Threading model
The broker itself runs on 3 threads. Namely:

| Thread | Owns | Notes |
|--------|------|-------|
| TcpGateway receiver | epoll loop, protocol decode, inbound queue producer | Graceful shutdown via `eventfd` |
| TcpGateway sender | Outbound SPSC consumer, TCP send | Blocks on dirty-fd queue; drains per-fd SPSC ring on notification |
| Router | Subscription table, inbound consumer, outbound producer | Sole owner of topic/fd maps — no mutex needed |

All threads are CPU-pinned via `pthread_setaffinity_np`. Stages communicate exclusively through lock-free queues — no shared mutable state between pipeline stages.
  
---

## Wire Protocol

Binary, little-endian. Fixed 24-byte header followed by a variable-length payload.

```
Offset  Size  Field
──────  ────  ─────────────────────────────────────
 0      2     magic         0xBEEF (frame sync)
 2      1     version       1
 3      1     type          SUBSCRIBE | UNSUBSCRIBE | PUBLISH | ACK | ERROR
 4      1     flags         RETAIN | NO_ACK | COMPRESSED (bitmask)
 5      3     reserved0     (zero on send, ignored on receive)
 8      8     sequence      per-connection monotonic counter
16      4     payload_len   bytes following the header
20      4     reserved1     (zero; CRC32 planned)
```

**Payload layouts:**

| Message type | Payload |
|---|---|
| `SUBSCRIBE` / `UNSUBSCRIBE` | `topic_len` (2B) + topic bytes |
| `PUBLISH` | `topic_len` (2B) + topic bytes + message body |
| `ACK` | `acked_seq` (8B) |
| `ERROR` | `error_code` (2B) + `msg_length` (2B) + UTF-8 string |

All encoding and decoding is zero-allocation. Callers provide pre-allocated `std::span` buffers; `decode_frame()` returns zero-copy `string_view` / `span` views into the receive buffer.

---

## Building

**Requirements:** Compiler that supports C++23, CMake 3.25+, Linux (epoll). All library dependencies are fetched automatically via CMake `FetchContent`.

```bash
# Clone and build (release mode)
git clone <repo-url>

cmake -S . -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release
```

Binaries are emitted to the build directory:

| Binary | Description |
|---|---|
| `broker` | The actual broker server |
| `load_gen` | Load generator + latency benchmark |
| `stress_test` | Multi-topic stress test with optional connection churn as well|
| `publisher` | Continuous publisher at a configurable rate |
| `subscriber` | Subscribe and print/count received messages |
| `broker_tests` | GTest unit test suite |

---

## Running

```bash
# Default settings (port 9000)
./cmake-build-debug/broker

# With TOML config (port, CPU pinning, connection limits)
./cmake-build-debug/broker config/broker.toml
```


### Demo

```bash
# Terminal 1: start broker
./cmake-build-debug/broker config/broker.toml

# Terminal 2: subscribe to a topic
./cmake-build-debug/subscriber 127.0.0.1 9000 market/aapl

# Terminal 3: publish at 10k msg/s
./cmake-build-debug/publisher 127.0.0.1 9000 market/aapl 10000
```

---

## Observability

The broker exposes a Prometheus `/metrics` endpoint on port `9090`.

| Metric | Type | Description |
|---|---|---|
| `messages_published_total` | Counter | Per topic |
| `messages_dropped_total` | Counter | Per subscriber (backpressure events) |
| `subscriber_queue_depth` | Gauge | Per subscriber ring buffer fill level |
| `e2e_latency_microseconds` | Histogram | Buckets at 10/50/100/500µs |
| `subscriber_count` | Gauge | Active subscribers |

### Grafana dashboard

A Docker Compose stack is included. Prometheus scrapes the broker on the host; Grafana serves a pre-provisioned dashboard showing throughput, drop rate, queue depth, and subscriber count in real time.

```bash
cd message-broker
GRAFANA_ADMIN_USER=admin GRAFANA_ADMIN_PASSWORD=admin docker compose up -d
```

- Prometheus UI: [http://localhost:9091](http://localhost:9091)
- Grafana: [http://localhost:3000](http://localhost:3000)

---

## Tests

```bash
./cmake-build-debug/tests/broker_tests
```

The test suite covers protocol encoding/decoding (all message types, all error paths, flag validation, layout assertions), router subscription logic, outbound message dispatch, and TCP gateway integration.

---

## Dependencies used

| Library | Version | Purpose |
|---|---|---|
| [moodycamel/concurrentqueue](https://github.com/cameron314/concurrentqueue) | v1.0.4 | Lock-free MPSC queues used for data access between gateway and router threads |
| [google/googletest](https://github.com/google/googletest) | v1.17.0 | Unit tests |
| [google/benchmark](https://github.com/google/benchmark) | v1.9.5 | Microbenchmark suite (can remove this)|
| [gabime/spdlog](https://github.com/gabime/spdlog) | v1.15.3 | Structured logging to stdout and stderr |
| [jupp0r/prometheus-cpp](https://github.com/jupp0r/prometheus-cpp) | v1.3.0 | Prometheus metrics exporter |
| [marzer/tomlplusplus](https://github.com/marzer/tomlplusplus) | v3.4.0 | TOML config parsing |

All of these are fetched automatically at configure time via CMake `FetchContent`.

## License info here