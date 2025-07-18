package github.yuanlin.driver.rocketmq;

import github.yuanlin.core.*;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.Message;

import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;

import org.apache.rocketmq.shaded.com.google.common.util.concurrent.FutureCallback;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.Futures;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.ListenableFuture;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.MoreExecutors;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author yuanlin.zhou
 * @date 2025/7/18 14:18
 * @description RocketMQ 5.3.0 Driver implementation for benchmark testing
 * Using cloud-native architecture with proxy layer
 */
public class RocketMQDriver implements Driver {
    private static final Logger logger = Logger.getLogger(RocketMQDriver.class.getName());

    private String proxyEndpoints;
    private ClientServiceProvider provider;
    private ClientConfiguration clientConfiguration;
    private Map<String, Object> driverConfig;

    @Override
    public void initialize(Map<String, Object> config) throws Exception {
        this.driverConfig = config;
        this.proxyEndpoints = (String) config.get("proxy.endpoints");

        if (proxyEndpoints == null) {
            throw new IllegalArgumentException("proxy.endpoints is required for RocketMQ 5.3.0");
        }

        // 获取ClientServiceProvider实例
        this.provider = ClientServiceProvider.loadService();

        // 构建客户端配置
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder()
                .setEndpoints(proxyEndpoints);

        // 设置请求超时
        if (config.containsKey("request.timeout.ms")) {
            builder.setRequestTimeout(Duration.ofMillis(((Number)config.get("request.timeout.ms")).longValue()));
        } else {
            builder.setRequestTimeout(Duration.ofSeconds(30));
        }

        // 设置命名空间（可选）
        if (config.containsKey("namespace")) {
            builder.setNamespace((String) config.get("namespace"));
        }

        this.clientConfiguration = builder.build();

        logger.info("RocketMQDriver initialized with proxy endpoints: " + proxyEndpoints);
    }

    @Override
    public void createTopic(String topicName, int partitions) throws Exception {
        // RocketMQ 5.x 中topic通常由管理员通过mqadmin工具或控制台创建
        // 这里我们可以尝试创建一个producer来触发topic的自动创建（如果服务端支持）
        try {
            Producer producer = provider.newProducerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setTopics(topicName)
                    .build();

            // 发送一条测试消息来触发topic创建
            Message message = provider.newMessageBuilder()
                    .setTopic(topicName)
                    .setBody("topic-creation-test".getBytes(StandardCharsets.UTF_8))
                    .build();

            try {
                SendReceipt receipt = producer.send(message);
                logger.info("Topic " + topicName + " created/verified successfully");
            } catch (Exception e) {
                logger.warning("Failed to verify topic creation: " + e.getMessage());
            }

            producer.close();
        } catch (Exception e) {
            logger.warning("Topic creation may need to be done manually via mqadmin: " + topicName);
            // 在生产环境中，通常需要手动创建topic
        }
    }

    @Override
    public BenchmarkProducer createProducer(String topicName) throws Exception {
        return new RocketMQBenchmarkProducer(topicName, provider, clientConfiguration, driverConfig);
    }

    @Override
    public BenchmarkConsumer createConsumer(String topicName, String consumerGroup) throws Exception {
        return new RocketMQBenchmarkConsumer(topicName, consumerGroup, provider, clientConfiguration, driverConfig);
    }

    @Override
    public void cleanup() throws Exception {
        // ClientServiceProvider 不需要显式关闭
        logger.info("RocketMQDriver cleanup completed");
    }

    @Override
    public String getDriverName() {
        return "RocketMQ-5.3.0";
    }

    /**
     * RocketMQ 5.3.0 Producer Implementation
     */
    private static class RocketMQBenchmarkProducer implements BenchmarkProducer {
        private final Producer producer;
        private final String topicName;
        private final ClientServiceProvider provider;

        public RocketMQBenchmarkProducer(String topicName, ClientServiceProvider provider,
                                         ClientConfiguration clientConfiguration,
                                         Map<String, Object> driverConfig) throws Exception {
            this.topicName = topicName;
            this.provider = provider;

            // 创建Producer
            this.producer = provider.newProducerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setTopics(topicName)
                    .build();
        }

        @Override
        public void sendAsync(byte[] message, Callback callback) throws Exception {
            long sendTime = System.currentTimeMillis();
            String messageId = "key" + System.nanoTime();

            Message msg = provider.newMessageBuilder()
                    .setTopic(topicName)
                    .setKeys(messageId)
                    .setBody(message)
                    .build();

            CompletableFuture<SendReceipt> future = producer.sendAsync(msg);

            future.whenComplete((sendReceipt, throwable) -> {
                if (throwable != null) {
                    callback.onException();
                } else {
                    callback.onCompletion();
                }
            });
        }

        @Override
        public github.yuanlin.core.SendResult sendSync(byte[] message) throws Exception {
            long sendTime = System.currentTimeMillis();
            String messageId = "key" + System.nanoTime();

            Message msg = provider.newMessageBuilder()
                    .setTopic(topicName)
                    .setKeys(messageId)
                    .setBody(message)
                    .build();

            try {
                SendReceipt receipt = producer.send(msg);
                long receiveTime = System.currentTimeMillis();
                return new github.yuanlin.core.SendResult(
                        true, sendTime, receiveTime - sendTime, messageId, null
                );
            } catch (Exception e) {
                long receiveTime = System.currentTimeMillis();
                return new github.yuanlin.core.SendResult(
                        false, sendTime, receiveTime - sendTime, messageId, e
                );
            }
        }

        @Override
        public void close() throws Exception {
            producer.close();
        }
    }

    /**
     * RocketMQ 5.3.0 Consumer Implementation
     */
    private static class RocketMQBenchmarkConsumer implements BenchmarkConsumer {
        private final PushConsumer consumer;
        private final String topicName;
        private final BlockingQueue<github.yuanlin.core.ConsumeResult> messageQueue;
        private volatile boolean isRunning = true;

        public RocketMQBenchmarkConsumer(String topicName, String consumerGroup,
                                         ClientServiceProvider provider,
                                         ClientConfiguration clientConfiguration,
                                         Map<String, Object> driverConfig) throws Exception {
            this.topicName = topicName;
            this.messageQueue = new LinkedBlockingQueue<>();

            // 创建过滤表达式
            FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);

            // 创建PushConsumer
            this.consumer = provider.newPushConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup(consumerGroup)
                    .setSubscriptionExpressions(Collections.singletonMap(topicName, filterExpression))
                    .setMessageListener(messageView -> {
                        try {
                            long receiveTime = System.currentTimeMillis();

                            github.yuanlin.core.ConsumeResult result = new github.yuanlin.core.ConsumeResult(
                                    messageView.getBody().array(),
                                    messageView.getBornTimestamp(),
                                    receiveTime,
                                    messageView.getKeys().stream().findFirst().orElse("nokey"),
                            0, // RocketMQ 5.x 不直接暴露队列ID
                                    0  // RocketMQ 5.x 不直接暴露偏移量
                            );

                            messageQueue.offer(result);
                            return ConsumeResult.SUCCESS;
                        } catch (Exception e) {
                            logger.warning("Failed to process message: " + e.getMessage());
                            return ConsumeResult.FAILURE;
                        }
                    })
                    .build();
        }

        @Override
        public List<github.yuanlin.core.ConsumeResult> consume(long timeoutMs) throws Exception {
            List<github.yuanlin.core.ConsumeResult> results = new ArrayList<>();

            long endTime = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < endTime && isRunning) {
                long remainingTime = endTime - System.currentTimeMillis();
                if (remainingTime <= 0) {
                    break;
                }

                github.yuanlin.core.ConsumeResult result = messageQueue.poll(remainingTime, TimeUnit.MILLISECONDS);
                if (result != null) {
                    results.add(result);
                } else {
                    break;
                }
            }

            return results;
        }

        @Override
        public void commitOffset() throws Exception {
            // RocketMQ 5.x Push模式下自动提交消费确认
            // 消费成功后会自动ACK，不需要手动提交
        }

        @Override
        public void close() throws Exception {
            isRunning = false;
            consumer.close();
        }
    }
}