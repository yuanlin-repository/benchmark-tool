package github.yuanlin.core;

import java.util.List;

/**
 * @author yuanlin.zhou
 * @date 2025/7/9 12:42
 * @description TODO
 */
public interface BenchmarkConsumer {
    List<ConsumeResult> consume(long timeoutMs) throws Exception;

    void commitOffset() throws Exception;

    void close() throws Exception;
}
