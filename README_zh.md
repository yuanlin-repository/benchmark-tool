# benchmark-tool

[English](README.md) | **中文**

一个用于消息系统性能基准测试的工具。

## 技术栈

- 语言：Java 17
- 消息队列：Kafka（4.0）和 RocketMQ（5.3.2）
- 监控：Prometheus 和 Grafana
- 依赖管理：Maven
- IDE：IntelliJ IDEA

## 项目亮点

现有实验普遍存在以下局限性：

- **版本陈旧，功能覆盖不足**：针对 Kafka、RocketMQ 的基准测试研究大多基于旧版本，无法反映近期的架构与功能变化。例如，Kafka 已将元数据与集群管理从 Zookeeper 迁移至 KRaft，而大多数研究仍依赖 Zookeeper；RocketMQ 5.0 引入了云原生解耦架构，通过 Proxy 层将客户端访问与 Broker 存储分离，这一点在以往研究中同样未被考量。
- **测试环境受限，缺乏现代架构支持**：许多实验在私人机器或开发者笔记本（如 MacBook）上进行，缺乏对容器化环境（如 Docker）或云平台（如 AWS、Azure）的评估。
- **缺乏标准化与可复现性**：统一基准测试流程和公开数据集的缺失，影响了不同研究间结果的可比性与可复现性。

## 架构

<img width="1191" height="922" alt="Benchmark Tool Architecture drawio" src="https://github.com/user-attachments/assets/c8273ad9-ce1f-4abe-b55e-c33e0d73f239" />

该基准测试工具的架构主要由以下组件构成：

- **Coordinator（协调器）**：负责解析工作负载配置文件、控制测试生命周期（启动、停止），并调用 Worker 执行测试任务。
- **Worker（工作节点）**：执行实际的消息生产、消费及指标采集任务。
- **Driver（驱动层）**：封装不同消息系统的操作，包括建立连接、创建 Topic 以及实例化生产者和消费者。
- **BenchmarkProducer（基准生产者）**：向消息系统发布消息。
- **BenchmarkConsumer（基准消费者）**：从消息系统拉取并消费消息。
- **Prometheus-Client（Prometheus 客户端）**：采集指标数据，本地存储，并暴露 HTTP 接口供 Prometheus Server 抓取。
- **Prometheus-Server（Prometheus 服务端）**：抓取 Worker 暴露的指标数据。
- **Grafana**：展示自定义仪表盘（如延迟直方图、吞吐量折线图等）。

## 项目结构

```
benchmark-tool/
├── deploy/                        # 各系统的部署资源
│   ├── kafka/                     # Kafka 部署脚本与配置
│   ├── prometheus_grafana/        # Prometheus + Grafana 监控部署文件
│   └── rocketmq/                  # RocketMQ 部署脚本与配置
├── src/
│   ├── main/
│   │   ├── java/github/yuanlin/
│   │   │   ├── coordinator/       # 基准测试协调器模块
│   │   │   ├── core/              # 核心抽象与公共组件
│   │   │   ├── driver/            # 各消息系统专用驱动
│   │   │   ├── metrics/           # 指标采集与上报
│   │   │   ├── worker/            # 基准测试执行逻辑
│   │   │   └── Main.java          # 程序入口
│   │   └── resources/             # 工作负载与配置文件
│   │
│   └── test/java/github/yuanlin/  # 单元测试与集成测试
├── .gitignore
└── pom.xml                        # Maven 项目配置
```

## 快速开始

### 前置条件

1. Azure 虚拟机
2. JDK 1.8+
3. Docker

### 使用流程

**1. 拉取仓库**

```bash
git clone git@github.com:yuanlin-repository/benchmark-tool.git
```

**2. 部署消息系统**

```bash
cd benchmark-tool/deploy/kafka
docker compose up -d
```

**3. （可选）部署 Prometheus 和 Grafana**

```bash
cd benchmark-tool/deploy/prometheus_grafana
docker compose up -d
```

**4. （可选）配置 Grafana**

<img width="780" height="300" alt="image" src="https://github.com/user-attachments/assets/8f63d69f-96f2-484b-88be-ef2465ec031e" />

1. 登录 Grafana 管理员账号
2. 添加 Prometheus 数据源
3. 根据 `/benchmark-tool/grafana_dashboard.txt` 配置 Grafana 仪表盘

**5. 选择工作负载文件**

```
/benchmark-tool/src/main/resources/kafka-10KB-pc1.yml
```

**6. 启动基准测试**

```bash
mvn exec:java \
  -Dexec.mainClass="github.yuanlin.Main" \
  -Dexec.args="kafka-10KB-pc1.yml"
```

**7. 等待测试结果**

<img width="800" height="130" alt="image" src="https://github.com/user-attachments/assets/623be15e-bdc2-428e-ba95-45c8e8bfbc3e" />
