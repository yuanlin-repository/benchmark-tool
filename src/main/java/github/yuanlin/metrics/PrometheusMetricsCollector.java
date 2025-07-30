package github.yuanlin.metrics;

import io.prometheus.client.*;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author yuanlin.zhou
 * @date 2025/7/10 14:47
 * @description TODO
 */
public class PrometheusMetricsCollector {
    private static final String NAMESPACE = "mq_benchmark";

    private final HTTPServer server;
    private final int port;

    // Producer Metrics
    private final Counter producerMessagesSent;
    private final Counter producerMessagesError;
    private final Histogram producerLatency;
    private final Gauge producerThroughput;

    // Consumer Metrics
    private final Counter consumerMessagesReceived;
    private final Histogram consumerLatency;
    private final Gauge consumerThroughput;

    // 系统指标
    private final Gauge activeProducers;
    private final Gauge activeConsumers;

    // Local
    private final AtomicLong totalProducerMessages = new AtomicLong(0);
    private final AtomicLong totalConsumerMessages = new AtomicLong(0);
    private final AtomicLong lastProducerCount = new AtomicLong(0);
    private final AtomicLong lastConsumerCount = new AtomicLong(0);
    private final AtomicLong lastTimestamp = new AtomicLong(System.currentTimeMillis());

    public PrometheusMetricsCollector(int port) throws IOException {
        this.port = port;

        // 初始化指标
        producerMessagesSent = Counter.build()
                .namespace(NAMESPACE)
                .name("producer_messages_sent_total")
                .help("Total number of messages sent by producers")
                .labelNames("topic", "worker_id")
                .register();

        producerMessagesError = Counter.build()
                .namespace(NAMESPACE)
                .name("producer_messages_error_total")
                .help("Total number of producer errors")
                .labelNames("topic", "worker_id", "error_type")
                .register();

        producerLatency = Histogram.build()
                .namespace(NAMESPACE)
                .name("producer_latency_ms")
                .help("Producer message latency in milliseconds")
                .labelNames("topic", "worker_id")
                .buckets(1, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000)
                .register();

        producerThroughput = Gauge.build()
                .namespace(NAMESPACE)
                .name("producer_throughput_msg_per_sec")
                .help("Producer throughput in messages per second")
                .labelNames("topic", "worker_id")
                .register();

        consumerMessagesReceived = Counter.build()
                .namespace(NAMESPACE)
                .name("consumer_messages_received_total")
                .help("Total number of messages received by consumers")
                .labelNames("topic", "worker_id", "consumer_group")
                .register();

        consumerLatency = Histogram.build()
                .namespace(NAMESPACE)
                .name("consumer_latency_ms")
                .help("Consumer message latency in milliseconds")
                .labelNames("topic", "worker_id", "consumer_group")
                .buckets(1, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, 20000, 40000, 60000, 80000, 100000, 120000, 140000, 160000,180000,200000)
                .register();

        consumerThroughput = Gauge.build()
                .namespace(NAMESPACE)
                .name("consumer_throughput_msg_per_sec")
                .help("Consumer throughput in messages per second")
                .labelNames("topic", "worker_id", "consumer_group")
                .register();

        activeProducers = Gauge.build()
                .namespace(NAMESPACE)
                .name("active_producers")
                .help("Number of active producers")
                .labelNames("topic")
                .register();

        activeConsumers = Gauge.build()
                .namespace(NAMESPACE)
                .name("active_consumers")
                .help("Number of active consumers")
                .labelNames("topic", "consumer_group")
                .register();

        // 启用JVM默认指标
        DefaultExports.initialize();

        // 启动HTTP服务器
        server = new HTTPServer(port);
        System.out.println("Prometheus metrics server started on port " + port);
    }

    /**
     * 记录生产者发送消息成功
     */
    public void recordProducerMessageSent(String topic, String workerId, long latencyMs) {
        producerMessagesSent.labels(topic, workerId).inc();
        producerLatency.labels(topic, workerId).observe(latencyMs);
        totalProducerMessages.incrementAndGet();
    }

    /**
     * 记录生产者发送消息错误
     */
    public void recordProducerError(String topic, String workerId, String errorType) {
        producerMessagesError.labels(topic, workerId, errorType).inc();
    }

    /**
     * 记录消费者接收消息
     */
    public void recordConsumerMessageReceived(String topic, String workerId, String consumerGroup, long latencyMs) {
        consumerMessagesReceived.labels(topic, workerId, consumerGroup).inc();
        consumerLatency.labels(topic, workerId, consumerGroup).observe(latencyMs);
        totalConsumerMessages.incrementAndGet();
    }

    /**
     * 更新生产者数量
     */
    public void updateActiveProducers(String topic, int count) {
        activeProducers.labels(topic).set(count);
    }

    /**
     * 更新消费者数量
     */
    public void updateActiveConsumers(String topic, String consumerGroup, int count) {
        activeConsumers.labels(topic, consumerGroup).set(count);
    }

    /**
     * 计算并更新吞吐量
     */
    public void updateThroughput(String topic, String workerId, String consumerGroup) {
        long currentTime = System.currentTimeMillis();
        long timeDiff = currentTime - lastTimestamp.get();

        if (timeDiff >= 1000) { // 每秒更新一次
            long currentProducerCount = totalProducerMessages.get();
            long currentConsumerCount = totalConsumerMessages.get();

            double producerTps = (currentProducerCount - lastProducerCount.get()) * 1000.0 / timeDiff;
            double consumerTps = (currentConsumerCount - lastConsumerCount.get()) * 1000.0 / timeDiff;

            producerThroughput.labels(topic, workerId).set(producerTps);
            if (consumerGroup != null) {
                consumerThroughput.labels(topic, workerId, consumerGroup).set(consumerTps);
            }

            lastProducerCount.set(currentProducerCount);
            lastConsumerCount.set(currentConsumerCount);
            lastTimestamp.set(currentTime);
        }
    }

    /**
     * 获取当前统计信息
     */
    public MetricsSnapshot getSnapshot() {
        return new MetricsSnapshot(
                totalProducerMessages.get(),
                totalConsumerMessages.get(),
                System.currentTimeMillis()
        );
    }

    /**
     * 关闭指标收集器
     */
    public void close() {
        if (server != null) {
            server.stop();
            System.out.println("Prometheus metrics server stopped");
        }
    }

    /**
     * 指标快照
     */
    public static class MetricsSnapshot {
        private final long totalProducerMessages;
        private final long totalConsumerMessages;
        private final long timestamp;

        public MetricsSnapshot(long totalProducerMessages, long totalConsumerMessages, long timestamp) {
            this.totalProducerMessages = totalProducerMessages;
            this.totalConsumerMessages = totalConsumerMessages;
            this.timestamp = timestamp;
        }

        public long getTotalProducerMessages() { return totalProducerMessages; }
        public long getTotalConsumerMessages() { return totalConsumerMessages; }
        public long getTimestamp() { return timestamp; }
    }
}
