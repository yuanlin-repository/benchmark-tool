package github.yuanlin.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yuanlin.zhou
 * @date 2025/7/10 14:54
 * @description TODO
 */
public class LocalMetricsCollector {
    private final Map<String, LatencyTracker> latencyTrackers = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    public void recordLatency(String metricName, long latencyMs) {
        latencyTrackers.computeIfAbsent(metricName, k -> new LatencyTracker()).record(latencyMs);
    }

    public void incrementCounter(String metricName) {
        counters.computeIfAbsent(metricName, k -> new AtomicLong(0)).incrementAndGet();
    }

    public void incrementCounter(String metricName, long delta) {
        counters.computeIfAbsent(metricName, k -> new AtomicLong(0)).addAndGet(delta);
    }

    public LatencyStats getLatencyStats(String metricName) {
        LatencyTracker tracker = latencyTrackers.get(metricName);
        return tracker != null ? tracker.getStats() : new LatencyStats(0, 0, 0, 0);
    }

    public long getCounterValue(String metricName) {
        AtomicLong counter = counters.get(metricName);
        return counter != null ? counter.get() : 0;
    }

    public void reset() {
        latencyTrackers.clear();
        counters.clear();
    }

    /**
     * 延迟跟踪器
     */
    private static class LatencyTracker {
        private final AtomicLong count = new AtomicLong(0);
        private final AtomicLong sum = new AtomicLong(0);
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(0);

        public void record(long latencyMs) {
            count.incrementAndGet();
            sum.addAndGet(latencyMs);

            // 更新最小值
            long currentMin = min.get();
            while (latencyMs < currentMin) {
                if (min.compareAndSet(currentMin, latencyMs)) {
                    break;
                }
                currentMin = min.get();
            }

            // 更新最大值
            long currentMax = max.get();
            while (latencyMs > currentMax) {
                if (max.compareAndSet(currentMax, latencyMs)) {
                    break;
                }
                currentMax = max.get();
            }
        }

        public LatencyStats getStats() {
            long c = count.get();
            long s = sum.get();
            long minVal = min.get() == Long.MAX_VALUE ? 0 : min.get();
            long maxVal = max.get();

            return new LatencyStats(c, s, minVal, maxVal);
        }
    }

    /**
     * 延迟统计结果
     */
    public static class LatencyStats {
        private final long count;
        private final long sum;
        private final long min;
        private final long max;

        public LatencyStats(long count, long sum, long min, long max) {
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
        }

        public long getCount() {
            return count;
        }

        public long getSum() {
            return sum;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public double getAverage() {
            return count > 0 ? (double) sum / count : 0;
        }
    }
}
