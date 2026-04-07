package com.loomq.scheduler;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * WAL 写入器（调度器层）
 *
 * 桥接层：将调度器的语义化事件写入底层高性能 WAL。
 */
public class WalWriter {
    private static final Logger logger = LoggerFactory.getLogger(WalWriter.class);

    private final com.loomq.wal.WalWriter innerWriter;

    public WalWriter() throws IOException {
        WalConfig config = ConfigFactory.create(WalConfig.class);
        this.innerWriter = new com.loomq.wal.WalWriter(config, "default");
        this.innerWriter.start();
    }

    public WalWriter(com.loomq.wal.WalWriter innerWriter) {
        this.innerWriter = innerWriter;
    }

    /**
     * 写入 CREATE 事件
     */
    public void writeCreateEvent(String taskId, byte[] payload) {
        try {
            innerWriter.append(taskId, null, EventType.CREATE, System.currentTimeMillis(), payload);
            logger.debug("WAL CREATE event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write CREATE event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 SCHEDULE 事件
     */
    public void writeScheduleEvent(String taskId) {
        try {
            innerWriter.append(taskId, null, EventType.SCHEDULE, System.currentTimeMillis(), null);
            logger.debug("WAL SCHEDULE event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write SCHEDULE event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 READY 事件
     */
    public void writeReadyEvent(String taskId) {
        try {
            innerWriter.append(taskId, null, EventType.READY, System.currentTimeMillis(), null);
            logger.debug("WAL READY event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write READY event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 DISPATCH 事件
     */
    public void writeDispatchEvent(String taskId) {
        try {
            innerWriter.append(taskId, null, EventType.DISPATCH, System.currentTimeMillis(), null);
            logger.debug("WAL DISPATCH event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write DISPATCH event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 ACK 事件
     */
    public void writeAckEvent(String taskId) {
        try {
            innerWriter.append(taskId, null, EventType.ACK, System.currentTimeMillis(), null);
            logger.debug("WAL ACK event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write ACK event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 RETRY 事件
     */
    public void writeRetryEvent(String taskId, int retryCount, String error) {
        try {
            byte[] payload = error != null ? error.getBytes() : null;
            innerWriter.append(taskId, null, EventType.RETRY, System.currentTimeMillis(), payload);
            logger.debug("WAL RETRY event written for task: {}, retryCount={}", taskId, retryCount);
        } catch (IOException e) {
            logger.error("Failed to write RETRY event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 FAIL 事件
     */
    public void writeFailEvent(String taskId, String error) {
        try {
            byte[] payload = error != null ? error.getBytes() : null;
            innerWriter.append(taskId, null, EventType.FAIL, System.currentTimeMillis(), payload);
            logger.debug("WAL FAIL event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write FAIL event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 CANCEL 事件
     */
    public void writeCancelEvent(String taskId) {
        try {
            innerWriter.append(taskId, null, EventType.CANCEL, System.currentTimeMillis(), null);
            logger.debug("WAL CANCEL event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write CANCEL event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 EXPIRE 事件
     */
    public void writeExpireEvent(String taskId) {
        try {
            innerWriter.append(taskId, null, EventType.EXPIRE, System.currentTimeMillis(), null);
            logger.debug("WAL EXPIRE event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write EXPIRE event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 写入 DEAD_LETTER 事件
     */
    public void writeDeadLetterEvent(String taskId, String error) {
        try {
            byte[] payload = error != null ? error.getBytes() : null;
            innerWriter.append(taskId, null, EventType.DEAD_LETTER, System.currentTimeMillis(), payload);
            logger.debug("WAL DEAD_LETTER event written for task: {}", taskId);
        } catch (IOException e) {
            logger.error("Failed to write DEAD_LETTER event for task: {}", taskId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }

    /**
     * 关闭 WAL 写入器
     */
    public void close() {
        try {
            innerWriter.close();
            logger.info("WAL writer closed");
        } catch (IOException e) {
            logger.error("Failed to close WAL writer", e);
        }
    }
}
