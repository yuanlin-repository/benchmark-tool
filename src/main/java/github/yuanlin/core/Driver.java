package github.yuanlin.core;

import java.util.Map;

/**
 * @author yuanlin.zhou
 * @date 2025/7/9 12:42
 * @description TODO
 */
public interface Driver {
    void initialize(Map<String, Object> config) throws Exception;

    void createTopic(String topicName, int partitions) throws Exception;

    BenchmarkProducer createProducer(String topicName) throws Exception;

    BenchmarkConsumer createConsumer(String topicName, String consumerGroup) throws Exception;

    void cleanup() throws Exception;

    String getDriverName();
}
