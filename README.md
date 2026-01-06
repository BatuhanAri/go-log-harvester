A high-performance, asynchronous log collection system built with **Go**, **NATS**, **ClickHouse**, and **Grafana**.


What this system does

Services publish logs as JSON messages to NATS

A Go-based collector subscribes to log topics

Logs are buffered in memory

Logs are written to ClickHouse in batches for efficiency

This is a best-effort logging pipeline designed for observability and telemetry use cases.


## 🏗 Architecture

Applications / Services
        |
        |  (JSON logs)
        v
       NATS (Core)
        |
        |  Subscribe (logs.*)
        v
   Go Collector
        |
        |  Batch Insert (size / time based)
        v
   ClickHouse

* **Producer:** Simulates microservices generating high-volume logs.
* **Broker:** NATS handles asynchronous message buffering.
* **Collector:** A Go service that consumes logs and performs **Batch Inserts** into ClickHouse.
* **Storage:** ClickHouse for columnar storage and high compression.
* **Visualization:** Grafana for real-time observability.


## Tech Stack

* **Language:** Golang (1.21+)
* **Messaging:** NATS
* **Database:** ClickHouse
* **Visualization:** Grafana
* **Infrastructure:** Docker & Docker Compose

## How to Run

1.  **Clone the repo**
    ```bash
    git clone [https://github.com/USERNAME/go-log-harvester.git](https://github.com/USERNAME/go-log-harvester.git)
    cd go-log-harvester
    ```

2.  **Create .env file**
    ```bash
    cat <<EOF > .env
    CLICKHOUSE_ADDR=localhost:9000
    CLICKHOUSE_DB=default
    CLICKHOUSE_USER=default
    CLICKHOUSE_PASSWORD=test123
    NATS_URL=nats://localhost:4222
    EOF
    ```

3.  **Start Infrastructure**
    ```bash
    docker-compose up -d
    ```

4.  **Run Collector**
    ```bash
    go mod tidy
    go run main.go
    ```

5.  **Generate Traffic** (In another terminal)
    ```bash
    go run producer.go
    ```

6.  **Visualize**
    Go to `http://localhost:3000` (User: admin/admin).


7.  **Design Notes**
    Batch processing is used to reduce ClickHouse insert overhead
    Backpressure is handled by dropping logs instead of blocking
    Graceful shutdown ensures in-flight batches are flushed on exit

7.  **Future Improvements**
    NATS JetStream (durable consumers, acknowledgements)
    Retry & backoff on ClickHouse failures
    Dead-letter queue for malformed logs
    Metrics & monitoring (Prometheus)

