package com.loomq.wal;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import com.loomq.entity.TaskEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * WAL 引擎
 * 统一的 WAL 读写入口
 */
public class WalEngine implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WalEngine.class);

    private final WalConfig config;
    private final WalWriter writer;
    private volatile boolean started = false;

    public WalEngine(WalConfig config) throws IOException {
        this.config = config;
        this.writer = new WalWriter(config);
    }

    /**
     * 启动 WAL 引擎
     */
    public void start() {
        if (started) {
            return;
        }
        started = true;
        logger.info("WAL engine started, data dir: {}", config.dataDir());
    }

    /**
     * 写入任务事件
     */
    public long appendEvent(TaskEvent event) throws IOException {
        byte[] payload = event.getPayload() != null ? event.getPayload().getBytes() : new byte[0];
        return writer.append(
                event.getTaskId(),
                event.getEventId(), // 使用 eventId 作为 bizKey
                event.getEventType(),
                event.getEventTime(),
                payload
        );
    }

    /**
     * 写入原始记录
     */
    public long append(String taskId, String bizKey, EventType eventType, long eventTime, byte[] payload) throws IOException {
        return writer.append(taskId, bizKey, eventType, eventTime, payload);
    }

    /**
     * 写入字符串 payload
     */
    public long append(String taskId, String bizKey, EventType eventType, long eventTime, String payload) throws IOException {
        byte[] payloadBytes = payload != null ? payload.getBytes() : new byte[0];
        return writer.append(taskId, bizKey, eventType, eventTime, payloadBytes);
    }

    /**
     * 强制刷盘
     */
    public void flush() {
        writer.flush();
    }

    /**
     * 创建读取器（用于恢复）
     */
    public WalReader createReader(boolean safeMode) {
        List<WalSegment> segments = writer.getAllSegments();
        return new WalReader(segments, safeMode);
    }

    /**
     * 获取所有段文件
     */
    public List<WalSegment> getAllSegments() {
        return writer.getAllSegments();
    }

    /**
     * 获取总记录数
     */
    public long getTotalRecords() {
        return writer.getTotalRecords();
    }

    /**
     * 获取数据目录
     */
    public String getDataDir() {
        return config.dataDir();
    }

    /**
     * 停止 WAL 引擎
     */
    public void stop() {
        if (!started) {
            return;
        }
        writer.close();
        started = false;
        logger.info("WAL engine stopped");
    }

    @Override
    public void close() {
        stop();
    }
}
