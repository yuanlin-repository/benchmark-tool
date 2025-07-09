package github.yuanlin.core;

import java.util.Map;

/**
 * @author yuanlin.zhou
 * @date 2025/7/9 12:42
 * @description TODO
 */
public class WorkloadConfig {
    private String name;
    private String driverClass;
    private Map<String, Object> driverConfig;
    private ProducerConfig producerConfig;
    private ConsumerConfig consumerConfig;
    private TestConfig testConfig;

    public static class ProducerConfig {
        private int threadCount = 1;
        private int rateLimit = -1; // -1表示无限制
        private int messageSizeBytes = 1024;
        private String topicName;
        private int batchSize = 1;

        // Getters and Setters
        public int getThreadCount() {
            return threadCount;
        }

        public void setThreadCount(int threadCount) {
            this.threadCount = threadCount;
        }

        public int getRateLimit() {
            return rateLimit;
        }

        public void setRateLimit(int rateLimit) {
            this.rateLimit = rateLimit;
        }

        public int getMessageSizeBytes() {
            return messageSizeBytes;
        }

        public void setMessageSizeBytes(int messageSizeBytes) {
            this.messageSizeBytes = messageSizeBytes;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }

    public static class ConsumerConfig {
        private int threadCount = 1;
        private String topicName;
        private String consumerGroup;
        private int batchSize = 100;
        private long pollTimeoutMs = 1000;

        // Getters and Setters
        public int getThreadCount() {
            return threadCount;
        }

        public void setThreadCount(int threadCount) {
            this.threadCount = threadCount;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public long getPollTimeoutMs() {
            return pollTimeoutMs;
        }

        public void setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
        }
    }

    public static class TestConfig {
        private long durationMs = 60000; // 默认60秒
        private long warmupMs = 5000; // 默认5秒预热
        private int targetPartitions = 1;
        private boolean enableProducer = true;
        private boolean enableConsumer = true;

        // Getters and Setters
        public long getDurationMs() {
            return durationMs;
        }

        public void setDurationMs(long durationMs) {
            this.durationMs = durationMs;
        }

        public long getWarmupMs() {
            return warmupMs;
        }

        public void setWarmupMs(long warmupMs) {
            this.warmupMs = warmupMs;
        }

        public int getTargetPartitions() {
            return targetPartitions;
        }

        public void setTargetPartitions(int targetPartitions) {
            this.targetPartitions = targetPartitions;
        }

        public boolean isEnableProducer() {
            return enableProducer;
        }

        public void setEnableProducer(boolean enableProducer) {
            this.enableProducer = enableProducer;
        }

        public boolean isEnableConsumer() {
            return enableConsumer;
        }

        public void setEnableConsumer(boolean enableConsumer) {
            this.enableConsumer = enableConsumer;
        }
    }

    // Main class getters and setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public Map<String, Object> getDriverConfig() {
        return driverConfig;
    }

    public void setDriverConfig(Map<String, Object> driverConfig) {
        this.driverConfig = driverConfig;
    }

    public ProducerConfig getProducerConfig() {
        return producerConfig;
    }

    public void setProducerConfig(ProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
    }

    public ConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    public void setConsumerConfig(ConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public TestConfig getTestConfig() {
        return testConfig;
    }

    public void setTestConfig(TestConfig testConfig) {
        this.testConfig = testConfig;
    }
}

