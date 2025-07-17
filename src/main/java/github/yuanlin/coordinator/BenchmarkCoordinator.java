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

/**
 * @author yuanlin.zhou
 * @date 2025/7/10 15:05
 * @description TODO
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
//        if (running.compareAndSet(false, true)) {
//            logger.info("Starting multiple benchmarks");
//
//            try {
//                List<WorkloadConfig> configs = new ArrayList<>();
//                for (String configPath : configFilePaths) {
//                    configs.add(parseConfig(configPath));
//                }
//
//                // 并行运行多个测试
//                runBenchmarksParallel(configs);
//
//            } catch (Exception e) {
//                logger.log(Level.SEVERE, "Multiple benchmarks failed", e);
//                throw e;
//            } finally {
//                stopAllWorkers();
//                running.set(false);
//            }
//        }
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
//        File configFile = new File(configFilePath);
//        if (!configFile.exists()) {
//            throw new IOException("Config file not found: " + configFilePath);
//        }

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
        long totalProducerMessages = 0;
        long totalSuccessfulMessages = 0;
        long totalFailedMessages = 0;
        double totalProducerThroughput = 0;
        List<Double> producerLatencies = new ArrayList<>();

        // 汇总消费者统计
        long totalConsumerMessages = 0;
        double totalConsumerThroughput = 0;
        List<Double> consumerLatencies = new ArrayList<>();

        for (BenchmarkResult result : results) {
            if (result.getProducerStats() != null) {
                BenchmarkResult.ProducerStats stats = result.getProducerStats();
                totalProducerMessages += stats.getTotalMessages();
                totalSuccessfulMessages += stats.getSuccessfulMessages();
                totalFailedMessages += stats.getFailedMessages();
                totalProducerThroughput += stats.getThroughputMsgPerSec();
                producerLatencies.add(stats.getAverageLatencyMs());
            }

            if (result.getConsumerStats() != null) {
                BenchmarkResult.ConsumerStats stats = result.getConsumerStats();
                totalConsumerMessages += stats.getTotalMessages();
                totalConsumerThroughput += stats.getThroughputMsgPerSec();
                consumerLatencies.add(stats.getAverageLatencyMs());
            }
        }

        // 输出汇总统计
        logger.info("Producer Summary:");
        logger.info("  Total Messages: " + totalProducerMessages);
        logger.info("  Successful Messages: " + totalSuccessfulMessages);
        logger.info("  Failed Messages: " + totalFailedMessages);
        logger.info("  Total Throughput: " + totalProducerThroughput + " msg/sec");
        logger.info("  Average Latency: " +
                (producerLatencies.isEmpty() ? 0 :
                        producerLatencies.stream().mapToDouble(d -> d).average().orElse(0)) + " ms");

        logger.info("Consumer Summary:");
        logger.info("  Total Messages: " + totalConsumerMessages);
        logger.info("  Total Throughput: " + totalConsumerThroughput + " msg/sec");
        logger.info("  Average Latency: " +
                (consumerLatencies.isEmpty() ? 0 :
                        consumerLatencies.stream().mapToDouble(d -> d).average().orElse(0)) + " ms");

        logger.info("=== END SUMMARY ===");
    }

    private String formatResult(BenchmarkResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("Test: ").append(result.getTestName()).append("\n");

        if (result.getProducerStats() != null) {
            BenchmarkResult.ProducerStats stats = result.getProducerStats();
            sb.append("  Producer - Messages: ").append(stats.getTotalMessages())
                    .append(", Success: ").append(stats.getSuccessfulMessages())
                    .append(", Failed: ").append(stats.getFailedMessages())
                    .append(", Throughput: ").append(stats.getThroughputMsgPerSec()).append(" msg/sec")
                    .append(", Avg Latency: ").append(stats.getAverageLatencyMs()).append(" ms\n");
        }

        if (result.getConsumerStats() != null) {
            BenchmarkResult.ConsumerStats stats = result.getConsumerStats();
            sb.append("  Consumer - Messages: ").append(stats.getTotalMessages())
                    .append(", Throughput: ").append(stats.getThroughputMsgPerSec()).append(" msg/sec")
                    .append(", Avg Latency: ").append(stats.getAverageLatencyMs()).append(" ms\n");
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
}
