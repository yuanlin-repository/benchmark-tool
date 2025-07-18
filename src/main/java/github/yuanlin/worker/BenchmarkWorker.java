package github.yuanlin.worker;

import github.yuanlin.core.*;
import github.yuanlin.metrics.PrometheusMetricsCollector;
import github.yuanlin.metrics.LocalMetricsCollector;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * @author yuanlin.zhou
 * @date 2025/7/10 14:57
 * @description Benchmark Worker with improved rate limiting using Guava RateLimiter
 */
public class BenchmarkWorker {
    private static final Logger logger = Logger.getLogger(BenchmarkWorker.class.getName());

    private final String workerId;
    private final WorkloadConfig config;
    private final Driver driver;
    private final PrometheusMetricsCollector prometheusCollector;
    private final LocalMetricsCollector localCollector;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private final List<ProducerTask> producerTasks = new ArrayList<>();
    private final List<ConsumerTask> consumerTasks = new ArrayList<>();
    private final List<Future<?>> futures = new ArrayList<>();

    public BenchmarkWorker(String workerId, WorkloadConfig config, Driver driver,
                           PrometheusMetricsCollector prometheusCollector) {
        this.workerId = workerId;
        this.config = config;
        this.driver = driver;
        this.prometheusCollector = prometheusCollector;
        this.localCollector = new LocalMetricsCollector();
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "BenchmarkWorker-" + workerId + "-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动基准测试
     */
    public void start() throws Exception {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting benchmark worker: " + workerId);

            // 创建Topic
            if (config.getTestConfig().isEnableProducer()) {
                driver.createTopic(config.getProducerConfig().getTopicName(),
                        config.getTestConfig().getTargetPartitions());
            }

            // 启动消费者任务
            if (config.getTestConfig().isEnableConsumer()) {
                startConsumerTasks();
            }

            // 启动生产者任务
            if (config.getTestConfig().isEnableProducer()) {
                startProducerTasks();
            }

            logger.info("Benchmark worker started: " + workerId);
        }
    }

    /**
     * 停止基准测试
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping benchmark worker: " + workerId);

            // 停止所有任务
            stopAllTasks();

            // 等待所有任务完成
            waitForTasksCompletion();

            // 关闭线程池
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }

            stopped.set(true);
            logger.info("Benchmark worker stopped: " + workerId);
        }
    }

    /**
     * 获取测试结果
     */
    public BenchmarkResult getResult() {
        BenchmarkResult.ProducerStats producerStats = calculateProducerStats();
        BenchmarkResult.ConsumerStats consumerStats = calculateConsumerStats();

        return new BenchmarkResult(
                config.getName(),
                System.currentTimeMillis() - config.getTestConfig().getDurationMs(),
                System.currentTimeMillis(),
                producerStats,
                consumerStats
        );
    }

    /**
     * 检查Worker是否正在运行
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 检查Worker是否已停止
     */
    public boolean isStopped() {
        return stopped.get();
    }

    private void startProducerTasks() throws Exception {
        WorkloadConfig.ProducerConfig producerConfig = config.getProducerConfig();

        for (int i = 0; i < producerConfig.getThreadCount(); i++) {
            BenchmarkProducer producer = driver.createProducer(producerConfig.getTopicName());
            ProducerTask task = new ProducerTask(producer, producerConfig, i);
            producerTasks.add(task);

            Future<?> future = executorService.submit(task);
            futures.add(future);
        }

        // 更新Prometheus指标
        prometheusCollector.updateActiveProducers(producerConfig.getTopicName(),
                producerConfig.getThreadCount());
    }

    private void startConsumerTasks() throws Exception {
        WorkloadConfig.ConsumerConfig consumerConfig = config.getConsumerConfig();

        for (int i = 0; i < consumerConfig.getThreadCount(); i++) {
            BenchmarkConsumer consumer = driver.createConsumer(consumerConfig.getTopicName(),
                    consumerConfig.getConsumerGroup());
            ConsumerTask task = new ConsumerTask(consumer, consumerConfig, i);
            consumerTasks.add(task);

            Future<?> future = executorService.submit(task);
            futures.add(future);
        }

        // 更新Prometheus指标
        prometheusCollector.updateActiveConsumers(consumerConfig.getTopicName(),
                consumerConfig.getConsumerGroup(),
                consumerConfig.getThreadCount());
    }

    private void stopAllTasks() {
        // 停止所有生产者任务
        for (ProducerTask task : producerTasks) {
            task.stop();
        }

        // 停止所有消费者任务
        for (ConsumerTask task : consumerTasks) {
            task.stop();
        }

        // 取消所有Future
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }

    private void waitForTasksCompletion() {
        for (Future<?> future : futures) {
            try {
                future.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Task completion interrupted", e);
            }
        }
    }

    private BenchmarkResult.ProducerStats calculateProducerStats() {
        BenchmarkResult.ProducerStats stats = new BenchmarkResult.ProducerStats();

        long totalMessages = localCollector.getCounterValue("producer.total");
        long successfulMessages = localCollector.getCounterValue("producer.success");
        long failedMessages = localCollector.getCounterValue("producer.error");

        LocalMetricsCollector.LatencyStats latencyStats =
                localCollector.getLatencyStats("producer.latency");

        stats.setTotalMessages(totalMessages);
        stats.setSuccessfulMessages(successfulMessages);
        stats.setFailedMessages(failedMessages);
        stats.setAverageLatencyMs(latencyStats.getAverage());
        stats.setP99LatencyMs(latencyStats.getMax()); // 简化实现，实际应该计算p99

        long duration = config.getTestConfig().getDurationMs();
        stats.setThroughputMsgPerSec(totalMessages * 1000.0 / duration);

        return stats;
    }

    private BenchmarkResult.ConsumerStats calculateConsumerStats() {
        BenchmarkResult.ConsumerStats stats = new BenchmarkResult.ConsumerStats();

        long totalMessages = localCollector.getCounterValue("consumer.total");
        LocalMetricsCollector.LatencyStats latencyStats =
                localCollector.getLatencyStats("consumer.latency");

        stats.setTotalMessages(totalMessages);
        stats.setAverageLatencyMs(latencyStats.getAverage());

        long duration = config.getTestConfig().getDurationMs();
        stats.setThroughputMsgPerSec(totalMessages * 1000.0 / duration);

        return stats;
    }

    /**
     * Producer Task - 使用Guava RateLimiter
     */
    private class ProducerTask implements Runnable {
        private final BenchmarkProducer producer;
        private final WorkloadConfig.ProducerConfig config;
        private volatile byte[] msg;
        private final int taskId;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final RateLimiter rateLimiter;
        private final Random random = new Random();

        public ProducerTask(BenchmarkProducer producer, WorkloadConfig.ProducerConfig config, int taskId) {
            this.producer = producer;
            this.config = config;
            this.taskId = taskId;

            // 使用Guava RateLimiter创建限流器
            // 每个线程分配总速率的一部分
            this.rateLimiter = config.getRateLimit() > 0 ?
                    RateLimiter.create((double) config.getRateLimit() / config.getThreadCount()) : null;
        }

        @Override
        public void run() {
            logger.info("Producer task started: " + taskId);

            try {
                // warm up
                if (BenchmarkWorker.this.config.getTestConfig().getWarmupMs() > 0) {
                    warmup();
                }

                // formal benchmark
                runTest();

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Producer task error: " + taskId, e);
            } finally {
                try {
                    producer.close();
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Failed to close producer: " + taskId, e);
                }
            }

            logger.info("Producer task completed: " + taskId);
        }

        private void warmup() throws Exception {
            long warmupEnd = System.currentTimeMillis() +
                    BenchmarkWorker.this.config.getTestConfig().getWarmupMs();

            while (running.get() && System.currentTimeMillis() < warmupEnd) {
                sendMessage(false);

                // 使用Guava RateLimiter进行限流
                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }
            }
        }

        private void runTest() throws Exception {
            long testEnd = System.currentTimeMillis() +
                    BenchmarkWorker.this.config.getTestConfig().getDurationMs();

            while (running.get() && System.currentTimeMillis() < testEnd) {
                sendMessage(true);

                // 使用Guava RateLimiter进行限流
                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }
            }
        }

        private void sendMessage(boolean recordStats) throws Exception {
            byte[] message = generateMessage();
            long startTime = System.currentTimeMillis();

            try {
                producer.sendAsync(message, new PerfCallback(startTime, config.getTopicName(), workerId,
                        recordStats, prometheusCollector, localCollector));
            } catch (Exception e) {
                throw e;
            }
        }

        private byte[] generateMessage() {
            if (msg == null) {
                msg = new byte[config.getMessageSizeBytes()];
                random.nextBytes(msg);
            }

            // 添加时间戳用于计算端到端延迟
            String timestamp = String.valueOf(System.currentTimeMillis());
            byte[] timestampBytes = timestamp.getBytes();
            System.arraycopy(timestampBytes, 0, msg, 0,
                    Math.min(timestampBytes.length, msg.length));

            return msg;
        }

        public void stop() {
            running.set(false);
        }
    }

    /**
     * 消费者任务
     */
    private class ConsumerTask implements Runnable {
        private final BenchmarkConsumer consumer;
        private final WorkloadConfig.ConsumerConfig config;
        private final int taskId;
        private final AtomicBoolean running = new AtomicBoolean(true);

        public ConsumerTask(BenchmarkConsumer consumer, WorkloadConfig.ConsumerConfig config, int taskId) {
            this.consumer = consumer;
            this.config = config;
            this.taskId = taskId;
        }

        @Override
        public void run() {
            logger.info("Consumer task started: " + taskId);

            try {
                // warm up
                if (BenchmarkWorker.this.config.getTestConfig().getWarmupMs() > 0) {
                    warmup();
                }

                // formal benchmark
                runTest();

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Consumer task error: " + taskId, e);
            } finally {
                try {
                    consumer.close();
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Failed to close consumer: " + taskId, e);
                }
            }

            logger.info("Consumer task completed: " + taskId);
        }

        private void warmup() throws Exception {
            long warmupEnd = System.currentTimeMillis() +
                    BenchmarkWorker.this.config.getTestConfig().getWarmupMs();

            while (running.get() && System.currentTimeMillis() < warmupEnd) {
                consumeMessages(false);
            }
        }

        private void runTest() throws Exception {
            long testEnd = System.currentTimeMillis() +
                    BenchmarkWorker.this.config.getTestConfig().getDurationMs();

            while (running.get() && System.currentTimeMillis() < testEnd) {
                consumeMessages(true);
            }
        }

        private void consumeMessages(boolean recordStats) throws Exception {
            List<ConsumeResult> results = consumer.consume(config.getPollTimeoutMs());

            for (ConsumeResult result : results) {
                long currentTime = System.currentTimeMillis();
                long latency = calculateLatency(result.getMessage(), currentTime);

                if (recordStats) {
                    localCollector.incrementCounter("consumer.total");
                    localCollector.recordLatency("consumer.latency", latency);

                    // 记录到Prometheus
                    prometheusCollector.recordConsumerMessageReceived(
                            config.getTopicName(), workerId, config.getConsumerGroup(), latency);
                    prometheusCollector.updateThroughput(config.getTopicName(), workerId, config.getConsumerGroup());
                }
            }

            if (!results.isEmpty()) {
                consumer.commitOffset();
            }
        }

        private long calculateLatency(byte[] message, long receiveTime) {
            try {
                String timestampStr = new String(message, 0,
                        Math.min(13, message.length)); // 时间戳长度约13位
                long sendTime = Long.parseLong(timestampStr.trim());
                return receiveTime - sendTime;
            } catch (Exception e) {
                return 0;
            }
        }

        public void stop() {
            running.set(false);
        }
    }

    static final class PerfCallback implements Callback {
        private final long start;
        private String topicName;
        private String workerId;
        private final boolean recordStats;
        private final PrometheusMetricsCollector prometheusCollector;
        private final LocalMetricsCollector localCollector;

        public PerfCallback(long start, String topicName, String workerId, boolean recordStats,
                            PrometheusMetricsCollector prometheusCollector, LocalMetricsCollector localCollector) {
            this.start = start;
            this.topicName = topicName;
            this.workerId = workerId;
            this.recordStats = recordStats;
            this.localCollector = localCollector;
            this.prometheusCollector = prometheusCollector;
        }

        @Override
        public void onCompletion() {
            long latency = System.currentTimeMillis() - start;

            if (recordStats) {
                localCollector.incrementCounter("producer.total");
                localCollector.incrementCounter("producer.success");
                localCollector.recordLatency("producer.latency", latency);

                // 记录到Prometheus
                prometheusCollector.recordProducerMessageSent(topicName, workerId, latency);
            }
        }

        @Override
        public void onException() {
            if (recordStats) {
                localCollector.incrementCounter("producer.error");
                prometheusCollector.recordProducerError(
                        topicName, workerId, "send_failed");
            }
        }
    }
}