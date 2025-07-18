package github.yuanlin.core;

/**
 * @author yuanlin.zhou
 * @date 2025/7/9 12:42
 * @description TODO
 */
public interface BenchmarkProducer {
    void sendAsync(byte[] message, Callback callback) throws Exception;

    SendResult sendSync(byte[] message) throws Exception;

    void close() throws Exception;
}