package github.yuanlin.coordinator;

import github.yuanlin.core.*;
import github.yuanlin.worker.BenchmarkWorker;
import github.yuanlin.metrics.PrometheusMetricsCollector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * @author yuanlin.zhou
 * @date 2025/7/10 15:05
 * @description Enhanced BenchmarkCoordinator with percentile latency aggregation
 */
public class BenchmarkCoordinator {
    private static final Logger logger = Logger.getLogger(BenchmarkCoordinator.class.getName());

    private final Map<String, BenchmarkWorker> workers = new ConcurrentHashMap<>();
    private final Map<String, Driver> drivers = new ConcurrentHashMap<>();
    private final PrometheusMetricsCollector metricsCollector;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    public BenchmarkCoordinator(int metricsPort) throws IOException {
        this.metricsCollector = new PrometheusMetricsCollector(metricsPort);
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "BenchmarkCoordinator-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });

        // 注册JVM关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /**
     * 从配置文件启动基准测试
     */
    public void startBenchmark(String configFilePath) throws Exception {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting benchmark from config: " + configFilePath);

            try {
                // 解析配置文件
                WorkloadConfig config = parseConfig(configFilePath);

                // 创建和启动Worker
                createAndStartWorkers(config);

                // 等待测试完成
                waitForTestCompletion(config);

                // 收集和输出结果
                collectAndOutputResults();

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Benchmark failed", e);
                throw e;
            } finally {
                stopAllWorkers();
                running.set(false);
            }
        }
    }

    /**
     * 启动多个基准测试
     */
    public void startBenchmarks(List<String> configFilePaths) throws Exception {
        for (String configFilePath : configFilePaths) {
            if (running.compareAndSet(false, true)) {
                logger.info("/------------------- Starting benchmark from config: " + configFilePath + "-------------/");

                try {
                    // 解析配置文件
                    WorkloadConfig config = parseConfig(configFilePath);

                    // 创建和启动Worker
                    createAndStartWorkers(config);

                    // 等待测试完成
                    waitForTestCompletion(config);

                    // 收集和输出结果
                    collectAndOutputResults();

                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Benchmark failed", e);
                    throw e;
                } finally {
                    stopAllWorkers();
                    running.set(false);
                }
            }
            logger.log(Level.INFO, "/------------------- Finish " + configFilePath + "test -------------/");
            Thread.sleep(10000);
        }
    }

    /**
     * 停止所有基准测试
     */
    public void stopAllBenchmarks() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping all benchmarks");
            stopAllWorkers();
        }
    }

    /**
     * 获取当前运行状态
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 获取活跃的Worker数量
     */
    public int getActiveWorkerCount() {
        return (int) workers.values().stream().filter(BenchmarkWorker::isRunning).count();
    }

    /**
     * 关闭协调器
     */
    public void shutdown() {
        logger.info("Shutting down benchmark coordinator");

        stopAllWorkers();

        // 关闭线程池
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        // 关闭驱动程序
        for (Driver driver : drivers.values()) {
            try {
                driver.cleanup();
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to cleanup driver", e);
            }
        }

        // 关闭指标收集器
        metricsCollector.close();

        logger.info("Benchmark coordinator shutdown complete");
    }

    private WorkloadConfig parseConfig(String configFilePath) throws IOException {
        InputStream inputStream = BenchmarkCoordinator.class.getClassLoader().getResourceAsStream(configFilePath);
        if (inputStream == null) {
            throw new IOException("Resource file " + configFilePath + " not found in classpath.");
        }

        return yamlMapper.readValue(inputStream, WorkloadConfig.class);
    }

    private void createAndStartWorkers(WorkloadConfig config) throws Exception {
        // 创建驱动程序
        Driver driver = createDriver(config);

        // 创建Worker
        String workerId = generateWorkerId(config.getName());
        BenchmarkWorker worker = new BenchmarkWorker(workerId, config, driver, metricsCollector);
        workers.put(workerId, worker);

        // 启动Worker
        worker.start();

        logger.info("Created and started worker: " + workerId);
    }

    private void runBenchmarksParallel(List<WorkloadConfig> configs) throws Exception {
        List<Future<Void>> futures = new ArrayList<>();

        for (WorkloadConfig config : configs) {
            Future<Void> future = executorService.submit(() -> {
                try {
                    // 创建驱动程序
                    Driver driver = createDriver(config);

                    // 创建Worker
                    String workerId = generateWorkerId(config.getName());
                    BenchmarkWorker worker = new BenchmarkWorker(workerId, config, driver, metricsCollector);
                    workers.put(workerId, worker);

                    // 启动Worker
                    worker.start();

                    // 等待测试完成
                    waitForSingleTestCompletion(config, worker);

                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Benchmark failed for config: " + config.getName(), e);
                    throw new RuntimeException(e);
                }
                return null;
            });

            futures.add(future);
        }

        // 等待所有测试完成
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Benchmark execution failed", e);
                throw new RuntimeException(e);
            }
        }

        // 收集和输出结果
        collectAndOutputResults();
    }

    private Driver createDriver(WorkloadConfig config) throws Exception {
        String driverClass = config.getDriverClass();

        // 检查是否已创建过相同的驱动程序
        Driver existingDriver = drivers.get(driverClass);
        if (existingDriver != null) {
            return existingDriver;
        }

        // 创建新的驱动程序
        Class<?> clazz = Class.forName(driverClass);
        Driver driver = (Driver) clazz.getDeclaredConstructor().newInstance();
        driver.initialize(config.getDriverConfig());

        drivers.put(driverClass, driver);
        logger.info("Created driver: " + driverClass);

        return driver;
    }

    private void waitForTestCompletion(WorkloadConfig config) throws InterruptedException {
        long testDuration = config.getTestConfig().getDurationMs();
        long warmupDuration = config.getTestConfig().getWarmupMs();
        long totalDuration = testDuration + warmupDuration;

        logger.info("Waiting for test completion, duration: " + totalDuration + "ms");

        // 等待测试完成
        Thread.sleep(totalDuration);

        // 给一些额外时间让Worker完成清理
        Thread.sleep(5000);
    }

    private void waitForSingleTestCompletion(WorkloadConfig config, BenchmarkWorker worker)
            throws InterruptedException {
        long testDuration = config.getTestConfig().getDurationMs();
        long warmupDuration = config.getTestConfig().getWarmupMs();
        long totalDuration = testDuration + warmupDuration;

        // 等待测试完成
        Thread.sleep(totalDuration);

        // 停止Worker
        worker.stop();

        // 等待Worker完全停止
        while (!worker.isStopped()) {
            Thread.sleep(100);
        }
    }

    private void collectAndOutputResults() {
        logger.info("Collecting benchmark results");

        List<BenchmarkResult> results = new ArrayList<>();

        for (Map.Entry<String, BenchmarkWorker> entry : workers.entrySet()) {
            String workerId = entry.getKey();
            BenchmarkWorker worker = entry.getValue();

            try {
                BenchmarkResult result = worker.getResult();
                results.add(result);

                logger.info("Result for worker " + workerId + ": " + formatResult(result));

            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to get result for worker: " + workerId, e);
            }
        }

        // 输出汇总结果
        outputSummaryResults(results);
    }

    private void outputSummaryResults(List<BenchmarkResult> results) {
        if (results.isEmpty()) {
            logger.warning("No results to summarize");
            return;
        }

        logger.info("=== BENCHMARK SUMMARY ===");

        // 汇总生产者统计
        ProducerAggregatedStats producerAggStats = aggregateProducerStats(results);
        outputProducerSummary(producerAggStats);

        // 汇总消费者统计
        ConsumerAggregatedStats consumerAggStats = aggregateConsumerStats(results);
        outputConsumerSummary(consumerAggStats);

        logger.info("=== END SUMMARY ===");
    }

    /**
     * 汇总生产者统计信息
     */
    private ProducerAggregatedStats aggregateProducerStats(List<BenchmarkResult> results) {
        ProducerAggregatedStats aggStats = new ProducerAggregatedStats();

        for (BenchmarkResult result : results) {
            if (result.getProducerStats() != null) {
                BenchmarkResult.ProducerStats stats = result.getProducerStats();

                aggStats.totalMessages += stats.getTotalMessages();
                aggStats.successfulMessages += stats.getSuccessfulMessages();
                aggStats.failedMessages += stats.getFailedMessages();
                aggStats.totalThroughput += stats.getThroughputMsgPerSec();

                // 收集所有延迟数据用于百分位数计算
                aggStats.latencies.add(stats.getAverageLatencyMs());
                if (stats.getP99LatencyMs() > 0) {
                    aggStats.p99Latencies.add(stats.getP99LatencyMs());
                }
            }
        }

        return aggStats;
    }

    /**
     * 汇总消费者统计信息
     */
    private ConsumerAggregatedStats aggregateConsumerStats(List<BenchmarkResult> results) {
        ConsumerAggregatedStats aggStats = new ConsumerAggregatedStats();

        for (BenchmarkResult result : results) {
            if (result.getConsumerStats() != null) {
                BenchmarkResult.ConsumerStats stats = result.getConsumerStats();

                aggStats.totalMessages += stats.getTotalMessages();
                aggStats.totalThroughput += stats.getThroughputMsgPerSec();

                // 收集所有延迟数据用于百分位数计算
                aggStats.latencies.add(stats.getAverageLatencyMs());

                if (stats.getP50LatencyMs() > 0) {
                    aggStats.p50Latencies.add((double) stats.getP50LatencyMs());
                }
                if (stats.getP90LatencyMs() > 0) {
                    aggStats.p90Latencies.add((double) stats.getP90LatencyMs());
                }
                if (stats.getP99LatencyMs() > 0) {
                    aggStats.p99Latencies.add((double) stats.getP99LatencyMs());
                }
            }
        }

        return aggStats;
    }

    /**
     * 输出生产者汇总结果
     */
    private void outputProducerSummary(ProducerAggregatedStats stats) {
        logger.info("Producer Summary:");
        logger.info("  Total Messages: " + stats.totalMessages);
        logger.info("  Successful Messages: " + stats.successfulMessages);
        logger.info("  Failed Messages: " + stats.failedMessages);
        logger.info("  Total Throughput: " + String.format("%.2f", stats.totalThroughput) + " msg/sec");

        if (!stats.latencies.isEmpty()) {
            double avgLatency = stats.latencies.stream().mapToDouble(d -> d).average().orElse(0);
            logger.info("  Average Latency: " + String.format("%.2f", avgLatency) + " ms");
        }

        if (!stats.p99Latencies.isEmpty()) {
            PercentileStats percentiles = calculatePercentiles(stats.p99Latencies);
            logger.info("  P50 Latency: " + String.format("%.2f", percentiles.p50) + " ms");
            logger.info("  P90 Latency: " + String.format("%.2f", percentiles.p90) + " ms");
            logger.info("  P99 Latency: " + String.format("%.2f", percentiles.p99) + " ms");
        }
    }

    /**
     * 输出消费者汇总结果
     */
    private void outputConsumerSummary(ConsumerAggregatedStats stats) {
        logger.info("Consumer Summary:");
        logger.info("  Total Messages: " + stats.totalMessages);
        logger.info("  Total Throughput: " + String.format("%.2f", stats.totalThroughput) + " msg/sec");

        if (!stats.latencies.isEmpty()) {
            double avgLatency = stats.latencies.stream().mapToDouble(d -> d).average().orElse(0);
            logger.info("  Average Latency: " + String.format("%.2f", avgLatency) + " ms");
        }

        // 输出聚合的百分位数延迟
        if (!stats.p50Latencies.isEmpty()) {
            PercentileStats p50Stats = calculatePercentiles(stats.p50Latencies);
            logger.info("  Aggregated P50 Latency:");
            logger.info("    Min: " + String.format("%.2f", Collections.min(stats.p50Latencies)) + " ms");
            logger.info("    Max: " + String.format("%.2f", Collections.max(stats.p50Latencies)) + " ms");
            logger.info("    Avg: " + String.format("%.2f", stats.p50Latencies.stream().mapToDouble(d -> d).average().orElse(0)) + " ms");
        }

        if (!stats.p90Latencies.isEmpty()) {
            PercentileStats p90Stats = calculatePercentiles(stats.p90Latencies);
            logger.info("  Aggregated P90 Latency:");
            logger.info("    Min: " + String.format("%.2f", Collections.min(stats.p90Latencies)) + " ms");
            logger.info("    Max: " + String.format("%.2f", Collections.max(stats.p90Latencies)) + " ms");
            logger.info("    Avg: " + String.format("%.2f", stats.p90Latencies.stream().mapToDouble(d -> d).average().orElse(0)) + " ms");
        }

        if (!stats.p99Latencies.isEmpty()) {
            PercentileStats p99Stats = calculatePercentiles(stats.p99Latencies);
            logger.info("  Aggregated P99 Latency:");
            logger.info("    Min: " + String.format("%.2f", Collections.min(stats.p99Latencies)) + " ms");
            logger.info("    Max: " + String.format("%.2f", Collections.max(stats.p99Latencies)) + " ms");
            logger.info("    Avg: " + String.format("%.2f", stats.p99Latencies.stream().mapToDouble(d -> d).average().orElse(0)) + " ms");
        }
    }

    /**
     * 计算延迟数据的百分位数
     */
    private PercentileStats calculatePercentiles(List<Double> latencies) {
        if (latencies.isEmpty()) {
            return new PercentileStats(0, 0, 0);
        }

        List<Double> sortedLatencies = new ArrayList<>(latencies);
        Collections.sort(sortedLatencies);

        int size = sortedLatencies.size();
        int p50Index = Math.max(0, (int) Math.ceil(size * 0.5) - 1);
        int p90Index = Math.max(0, (int) Math.ceil(size * 0.9) - 1);
        int p99Index = Math.max(0, (int) Math.ceil(size * 0.99) - 1);

        double p50 = sortedLatencies.get(p50Index);
        double p90 = sortedLatencies.get(p90Index);
        double p99 = sortedLatencies.get(p99Index);

        return new PercentileStats(p50, p90, p99);
    }

    private String formatResult(BenchmarkResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("Test: ").append(result.getTestName()).append("\n");

        if (result.getProducerStats() != null) {
            BenchmarkResult.ProducerStats stats = result.getProducerStats();
            sb.append("  Producer - Messages: ").append(stats.getTotalMessages())
                    .append(", Success: ").append(stats.getSuccessfulMessages())
                    .append(", Failed: ").append(stats.getFailedMessages())
                    .append(", Throughput: ").append(String.format("%.2f", stats.getThroughputMsgPerSec())).append(" msg/sec")
                    .append(", Avg Latency: ").append(String.format("%.2f", stats.getAverageLatencyMs())).append(" ms")
                    .append(", P99 Latency: ").append(String.format("%.2f", stats.getP99LatencyMs())).append(" ms\n");
        }

        if (result.getConsumerStats() != null) {
            BenchmarkResult.ConsumerStats stats = result.getConsumerStats();
            sb.append("  Consumer - Messages: ").append(stats.getTotalMessages())
                    .append(", Throughput: ").append(String.format("%.2f", stats.getThroughputMsgPerSec())).append(" msg/sec")
                    .append(", Avg Latency: ").append(String.format("%.2f", stats.getAverageLatencyMs())).append(" ms")
                    .append(", P50: ").append(String.format("%.2f", (double) stats.getP50LatencyMs())).append(" ms")
                    .append(", P90: ").append(String.format("%.2f", (double) stats.getP90LatencyMs())).append(" ms")
                    .append(", P99: ").append(String.format("%.2f", (double) stats.getP99LatencyMs())).append(" ms\n");
        }

        return sb.toString();
    }

    private void stopAllWorkers() {
        logger.info("Stopping all workers");

        for (Map.Entry<String, BenchmarkWorker> entry : workers.entrySet()) {
            String workerId = entry.getKey();
            BenchmarkWorker worker = entry.getValue();

            try {
                worker.stop();
                logger.info("Stopped worker: " + workerId);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to stop worker: " + workerId, e);
            }
        }

        workers.clear();
    }

    private String generateWorkerId(String configName) {
        return configName + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * 生产者聚合统计数据
     */
    private static class ProducerAggregatedStats {
        long totalMessages = 0;
        long successfulMessages = 0;
        long failedMessages = 0;
        double totalThroughput = 0;
        List<Double> latencies = new ArrayList<>();
        List<Double> p99Latencies = new ArrayList<>();
    }

    /**
     * 消费者聚合统计数据
     */
    private static class ConsumerAggregatedStats {
        long totalMessages = 0;
        double totalThroughput = 0;
        List<Double> latencies = new ArrayList<>();
        List<Double> p50Latencies = new ArrayList<>();
        List<Double> p90Latencies = new ArrayList<>();
        List<Double> p99Latencies = new ArrayList<>();
    }

    /**
     * 百分位数统计数据
     */
    private static class PercentileStats {
        final double p50;
        final double p90;
        final double p99;

        public PercentileStats(double p50, double p90, double p99) {
            this.p50 = p50;
            this.p90 = p90;
            this.p99 = p99;
        }
    }
}