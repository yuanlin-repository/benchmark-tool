package github.yuanlin.driver.kafka;

import github.yuanlin.core.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * @author yuanlin.zhou
 * @date 2025/7/10 15:30
 * @description Kafka Driver implementation for benchmark testing
 */
public class KafkaDriver implements Driver {
    private static final Logger logger = Logger.getLogger(KafkaDriver.class.getName());

    private String bootstrapServers;
    private AdminClient adminClient;
    private Map<String, Object> driverConfig;

    @Override
    public void initialize(Map<String, Object> config) throws Exception {
        this.driverConfig = config;
        this.bootstrapServers = (String) config.get("bootstrap.servers");

        if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrap.servers is required");
        }

        // 创建 AdminClient
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30000);

        this.adminClient = AdminClient.create(adminProps);

        logger.info("KafkaDriver initialized with bootstrap servers: " + bootstrapServers);
    }

    @Override
    public void createTopic(String topicName, int partitions) throws Exception {
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) -1);
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
            logger.info("Topic created successfully: " + topicName + " with " + partitions + " partitions");
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                logger.info("Topic already exists: " + topicName);
            } else {
                throw e;
            }
        }
    }

    @Override
    public BenchmarkProducer createProducer(String topicName) throws Exception {
        return new KafkaBenchmarkProducer(topicName, driverConfig);
    }

    @Override
    public BenchmarkConsumer createConsumer(String topicName, String consumerGroup) throws Exception {
        return new KafkaBenchmarkConsumer(topicName, consumerGroup, driverConfig);
    }

    @Override
    public void cleanup() throws Exception {
        if (adminClient != null) {
            adminClient.close();
        }
        logger.info("KafkaDriver cleanup completed");
    }

    @Override
    public String getDriverName() {
        return "Kafka";
    }

    /**
     * Kafka Producer Implementation
     */
    private static class KafkaBenchmarkProducer implements BenchmarkProducer {
        private final Producer<String, byte[]> producer;
        private final String topicName;

        public KafkaBenchmarkProducer(String topicName, Map<String, Object> driverConfig) {
            this.topicName = topicName;

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, driverConfig.get("bootstrap.servers"));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

            // 性能优化配置
//            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//            props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
//            props.put(ProducerConfig.RETRIES_CONFIG, 3);
//            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
//            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

            // 允许用户覆盖配置
            for (Map.Entry<String, Object> entry : driverConfig.entrySet()) {
                if (entry.getKey().startsWith("producer.")) {
                    String key = entry.getKey().substring("producer.".length());
                    props.put(key, entry.getValue());
                }
            }

            this.producer = new KafkaProducer<>(props);
        }

        @Override
        public void sendAsync(byte[] message, Callback callback) throws Exception {
            long sendTime = System.currentTimeMillis();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, message);
            producer.send(record, callback);
        }

        @Override
        public SendResult sendSync(byte[] message) throws Exception {
            long sendTime = System.currentTimeMillis();
            String messageId = "key" + System.nanoTime();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, messageId, message);

            try {
                Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                });
                long receiveTime = System.currentTimeMillis();
                return new SendResult(true, sendTime,
                        receiveTime - sendTime, messageId, null);
            } catch (Exception e) {
                long receiveTime = System.currentTimeMillis();
                return new SendResult(false, sendTime,
                        receiveTime - sendTime, messageId, e);
            }
        }

        @Override
        public void close() throws Exception {
            producer.close();
        }
    }

    /**
     * Kafka Consumer Implementation
     */
    private static class KafkaBenchmarkConsumer implements BenchmarkConsumer {
        private final Consumer<String, byte[]> consumer;
        private final String topicName;

        public KafkaBenchmarkConsumer(String topicName, String consumerGroup, Map<String, Object> driverConfig) {
            this.topicName = topicName;

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, driverConfig.get("bootstrap.servers"));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
//            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);      // 修正：合理的批量大小
//            props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);      // 1KB
//            props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
//            props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800);  // 50MB
//            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576); // 1MB
//            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
//            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);

            // 允许用户覆盖配置
            for (Map.Entry<String, Object> entry : driverConfig.entrySet()) {
                if (entry.getKey().startsWith("consumer.")) {
                    String key = entry.getKey().substring("consumer.".length());
                    props.put(key, entry.getValue());
                }
            }

            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(Collections.singletonList(topicName));
        }

        @Override
        public List<ConsumeResult> consume(long timeoutMs) throws Exception {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(timeoutMs));

            List<ConsumeResult> results = new ArrayList<>();
            long receiveTime = System.currentTimeMillis();

            for (ConsumerRecord<String, byte[]> record : records) {
                ConsumeResult result = new ConsumeResult(
                        record.value(),
                        record.timestamp(),
                        receiveTime,
                        record.key(),
                        record.partition(),
                        record.offset()
                );
                results.add(result);
            }

            return results;
        }

        @Override
        public void commitOffset() throws Exception {
            consumer.commitSync();
        }

        @Override
        public void close() throws Exception {
            consumer.close();
        }
    }
}