package github.yuanlin.core;

import java.util.concurrent.Future;

/**
 * @author yuanlin.zhou
 * @date 2025/7/9 12:42
 * @description TODO
 */
public interface BenchmarkProducer {
    Future<SendResult> sendAsync(byte[] message) throws Exception;

    SendResult sendSync(byte[] message) throws Exception;

    void close() throws Exception;
}