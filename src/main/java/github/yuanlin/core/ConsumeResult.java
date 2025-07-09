package github.yuanlin.core;

/**
 * @author yuanlin.zhou
 * @date 2025/7/9 12:42
 * @description TODO
 */
public class ConsumeResult {
    private final byte[] message;
    private final long timestamp;
    private final long receiveTime;
    private final String messageId;
    private final int partition;
    private final long offset;

    public ConsumeResult(byte[] message, long timestamp, long receiveTime,
                         String messageId, int partition, long offset) {
        this.message = message;
        this.timestamp = timestamp;
        this.receiveTime = receiveTime;
        this.messageId = messageId;
        this.partition = partition;
        this.offset = offset;
    }

    // Getters
    public byte[] getMessage() {
        return message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getReceiveTime() {
        return receiveTime;
    }

    public String getMessageId() {
        return messageId;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}
