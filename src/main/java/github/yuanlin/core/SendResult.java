package github.yuanlin.core;

/**
 * @author yuanlin.zhou
 * @date 2025/7/9 12:42
 * @description TODO
 */
public class SendResult {
    private final boolean success;
    private final long timestamp;
    private final long latencyMs;
    private final String messageId;
    private final Throwable error;

    public SendResult(boolean success, long timestamp, long latencyMs, String messageId, Throwable error) {
        this.success = success;
        this.timestamp = timestamp;
        this.latencyMs = latencyMs;
        this.messageId = messageId;
        this.error = error;
    }

    // Getters
    public boolean isSuccess() {
        return success;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public String getMessageId() {
        return messageId;
    }

    public Throwable getError() {
        return error;
    }
}
