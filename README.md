# Message Broker

A high-throughput, low-latency publish/subscribe message broker.
Acts as middleware to succesfully route messages to topic subscribers

---

## Threading model
The broker itself runs on 3 threads. Namely:
* TcpGateway receiver
* TcpGateway sender
* Router
  
---

## Wire Protocol

Binary, little-endian (can easily be changed to use big-endian if needed). Fixed 24-byte header followed by a variable-length payload.


| Offset | Size | Field      | Value(s)                                          |
|--------|------|------------|---------------------------------------------------|
| 0      | 2    | magic      | 0xBEEF (frame sync)                               |
| 2      | 1    | version    | 1                                                 |
| 3      | 1    | type       | SUBSCRIBE \| UNSUBSCRIBE \| PUBLISH \| ACK \| ERROR |
| 4      | 1    | flags      | RETAIN \| NO_ACK \| COMPRESSED (bitmask)        |
| 5      | 3    | reserved0  | (zero on send, ignored on receive)               |
| 8      | 8    | sequence   | per-connection monotonic counter                 |
| 16     | 4    | payload_len| bytes following the header                       |
| 20     | 4    | reserved1  | (zero; CRC32 planned)                            |



**Payload layouts of Messages:**

| Message type | Payload |
|---|---|
| `SUBSCRIBE` / `UNSUBSCRIBE` | `topic_len` (2B) + topic bytes |
| `PUBLISH` | `topic_len` (2B) + topic bytes + message body |
| `ACK` | `acked_seq` (8B) |
| `ERROR` | `error_code` (2B) + `msg_length` (2B) + error message string (UTF-8 supported) |


---

## Building

**Requirements:** Compiler that supports C++23, CMake 3.25+, Linux (epoll). All library dependencies are fetched automatically via CMake `FetchContent`.
The project was developed with clang in mind.

```bash
# Clone and build (release mode)
git clone https://github.com/leon-biju/message-broker

cmake --preset release
cmake --build cmake-build-release
```

The following binaries are created in the build directory:

| Binary | Description |
|---|---|
| `broker` | The actual broker program |
| `load_gen` | Load generator + latency benchmark |
| `stress_test` | Multi-topic stress test with optional connection churn as well|
| `publisher` | Continuous publisher at a configurable rate |
| `subscriber` | Subscribe and print/count received messages |
| `broker_tests` | GTest unit test suite |

---

## Running

```bash
# Default settings (port 9000)
./cmake-build-release/broker

# With TOML config (port, CPU pinning, etc.)
./cmake-build-release/broker config/broker.toml
```


### Demo tools

```bash
# Terminal 1: start broker
./cmake-build-release/broker config/broker.toml

# Terminal 2: subscribe to a topic
./cmake-build-release/subscriber 127.0.0.1 9000 market/aapl

# Terminal 3: publish at 10k msg/s
./cmake-build-release/publisher 127.0.0.1 9000 market/aapl 10000
```

---

## Observability

The broker exposes a Prometheus `/metrics` endpoint on port `9090`.

### Grafana dashboard

A Docker Compose stack is included (prometheus + grafana). Prometheus scrapes the broker on the host and Grafana serves a dashboard showing throughput, drop rate, etc. in real time.

```bash
cd message-broker
GRAFANA_ADMIN_USER=admin GRAFANA_ADMIN_PASSWORD=admin docker compose up -d
```

- Prometheus UI: [http://localhost:9091](http://localhost:9091)
- Grafana: [http://localhost:3000](http://localhost:3000)

---

## Tests

```bash
./cmake-build-release/tests/broker_tests
```

---

## Dependencies used

| Library | Version | Purpose |
|---|---|---|
| [moodycamel/concurrentqueue](https://github.com/cameron314/concurrentqueue) | v1.0.4 | Lock-free MPSC queues |
| [google/googletest](https://github.com/google/googletest) | v1.17.0 | Unit tests |
| [google/benchmark](https://github.com/google/benchmark) | v1.9.5 | Microbenchmark suite (can remove this)|
| [gabime/spdlog](https://github.com/gabime/spdlog) | v1.15.3 | Structured logging |
| [jupp0r/prometheus-cpp](https://github.com/jupp0r/prometheus-cpp) | v1.3.0 | Prometheus metrics exporter |
| [marzer/tomlplusplus](https://github.com/marzer/tomlplusplus) | v3.4.0 | TOML parsing |

All of these are fetched automatically at configure time via CMake `FetchContent`.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.