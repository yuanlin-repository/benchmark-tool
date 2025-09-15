# benchmark-tool

A tool to benchmarking messaging systems.

## Architecture
<img width="1191" height="922" alt="Benchmark Tool Architecture drawio" src="https://github.com/user-attachments/assets/c8273ad9-ce1f-4abe-b55e-c33e0d73f239" />

The architecture of the benchmarking tool is primarily comprises the following components:
•	Coordinator: Responsible for parsing the workload configuration file, controlling the test lifecycle (start, stop), and invoking Workers to execute test tasks.
•	Worker: Carries out the actual production, consumption, and metric collection tasks.
•	Driver: Encapsulates operations for different messaging systems, including establishing connections, creating topics, and instantiating producers and consumers.
•	BenchmarkProducer: Publishes messages to the messaging system.
•	BenchmarkConsumer: Retrieves and consumes messages from the messaging system.
•	Prometheus-Client: Collects metrics, stores them locally, and exposes an HTTP interface for the Prometheus-Server to scrape.
•	Prometheus-Server: Scrapes the metrics exposed by the Workers.
•	Grafana: Displays custom dashboards (e.g., latency histograms, throughput line charts, etc.).

## Project Structure

```
benchmark-tool/
├── deploy/                        # Deployment resources for different systems
│   ├── kafka/                     # Deployment scripts/configuration for Kafka
│   ├── prometheus_grafana/        # Deployment files for Prometheus + Grafana monitoring
│   └── rocketmq/                  # Deployment scripts/configuration for RocketMQ
├── src/
│   ├── main/
│   │   ├── java/github/yuanlin/
│   │   │   ├── coordinator/       # Benchmark coordinator module
│   │   │   ├── core/              # Core abstractions and shared components
│   │   │   ├── driver/            # Messaging system-specific drivers
│   │   │   ├── metrics/           # Metrics collection and reporting
│   │   │   ├── worker/            # Worker logic for benchmark execution
│   │   │   └── Main.java          # Main entry point of the benchmark tool
│   │   └── resources/             # Workload and configuration files
│   │
│   └── test/java/github/yuanlin/  # Unit and integration tests
├── .gitignore                     # Git ignore rules
└── pom.xml                        # Maven project configuration

```

## Quick Start

### Prerequisites
1. Azure VM
2. JDK1.8+
3. Docker

### Workflow 
1. Pull the repository
```
git clone git@github.com:yuanlin-repository/benchmark-tool.git
```

2. Deploy the messaging systems
```
cd benchmark-tool/deploy
cd kafka
docker compose up -d
```

3. (Optional) Deploy the prometheus and grafana
```
cd benchmark-tool/deploy
cd prometheus_grafana
docker compose up -d
```

4. (Optional)Config the grafana
<img width="780" height="300" alt="image" src="https://github.com/user-attachments/assets/8f63d69f-96f2-484b-88be-ef2465ec031e" />

1) login grafana admin
2) add prometheus data source
3) config grafana dashboard based on /benchmark-tool/grafana_dashboard.txt


5. Choose workload file
```
/benchmark-tool/src/main/resources/kafka-10KB-pc1.yml
```

6. Start benchmark
```
mvn exec:java \
  -Dexec.mainClass="github.yuanlin.Main" \
  -Dexec.args="kafka-10KB-pc1.yml"
```

7. Wait for benchmark results

<img width="800" height="130" alt="image" src="https://github.com/user-attachments/assets/623be15e-bdc2-428e-ba95-45c8e8bfbc3e" />

