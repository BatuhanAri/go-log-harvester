
A high-performance, asynchronous log collection system built with **Go**, **NATS**, **ClickHouse**, and **Grafana**.

## 🏗 Architecture

`Microservices (Simulated)` -> **NATS JetStream** -> **Go Collector** -> **ClickHouse** -> **Grafana**

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
    echo "CLICKHOUSE_PASSWORD=test123" > .env
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

