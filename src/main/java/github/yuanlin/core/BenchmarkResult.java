package github.yuanlin.core;

/**
 * @author yuanlin.zhou
 * @date 2025/7/9 12:42
 * @description TODO
 */
public class BenchmarkResult {
    private final String testName;
    private final long startTime;
    private final long endTime;
    private final ProducerStats producerStats;
    private final ConsumerStats consumerStats;

    public BenchmarkResult(String testName, long startTime, long endTime,
                           ProducerStats producerStats, ConsumerStats consumerStats) {
        this.testName = testName;
        this.startTime = startTime;
        this.endTime = endTime;
        this.producerStats = producerStats;
        this.consumerStats = consumerStats;
    }

    public static class ProducerStats {
        private long totalMessages;
        private long successfulMessages;
        private long failedMessages;
        private double averageLatencyMs;
        private double p99LatencyMs;
        private double throughputMsgPerSec;

        // Getters and Setters
        public long getTotalMessages() {
            return totalMessages;
        }

        public void setTotalMessages(long totalMessages) {
            this.totalMessages = totalMessages;
        }

        public long getSuccessfulMessages() {
            return successfulMessages;
        }

        public void setSuccessfulMessages(long successfulMessages) {
            this.successfulMessages = successfulMessages;
        }

        public long getFailedMessages() {
            return failedMessages;
        }

        public void setFailedMessages(long failedMessages) {
            this.failedMessages = failedMessages;
        }

        public double getAverageLatencyMs() {
            return averageLatencyMs;
        }

        public void setAverageLatencyMs(double averageLatencyMs) {
            this.averageLatencyMs = averageLatencyMs;
        }

        public double getP99LatencyMs() {
            return p99LatencyMs;
        }

        public void setP99LatencyMs(double p99LatencyMs) {
            this.p99LatencyMs = p99LatencyMs;
        }

        public double getThroughputMsgPerSec() {
            return throughputMsgPerSec;
        }

        public void setThroughputMsgPerSec(double throughputMsgPerSec) {
            this.throughputMsgPerSec = throughputMsgPerSec;
        }
    }

    public static class ConsumerStats {
        private long totalMessages;
        private double averageLatencyMs;
        private double throughputMsgPerSec;

        // New percentile latency fields
        private long p50LatencyMs;
        private long p90LatencyMs;
        private long p99LatencyMs;

        // Getters and Setters
        public long getTotalMessages() {
            return totalMessages;
        }

        public void setTotalMessages(long totalMessages) {
            this.totalMessages = totalMessages;
        }

        public double getAverageLatencyMs() {
            return averageLatencyMs;
        }

        public void setAverageLatencyMs(double averageLatencyMs) {
            this.averageLatencyMs = averageLatencyMs;
        }

        public double getThroughputMsgPerSec() {
            return throughputMsgPerSec;
        }

        public void setThroughputMsgPerSec(double throughputMsgPerSec) {
            this.throughputMsgPerSec = throughputMsgPerSec;
        }

        public long getP50LatencyMs() {
            return p50LatencyMs;
        }

        public void setP50LatencyMs(long p50LatencyMs) {
            this.p50LatencyMs = p50LatencyMs;
        }

        public long getP90LatencyMs() {
            return p90LatencyMs;
        }

        public void setP90LatencyMs(long p90LatencyMs) {
            this.p90LatencyMs = p90LatencyMs;
        }

        public long getP99LatencyMs() {
            return p99LatencyMs;
        }

        public void setP99LatencyMs(long p99LatencyMs) {
            this.p99LatencyMs = p99LatencyMs;
        }
    }

    // Getters
    public String getTestName() {
        return testName;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public ProducerStats getProducerStats() {
        return producerStats;
    }

    public ConsumerStats getConsumerStats() {
        return consumerStats;
    }
}
