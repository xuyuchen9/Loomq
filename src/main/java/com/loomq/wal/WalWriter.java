package com.loomq.wal;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WAL 写入器
 * 支持 per_record, batch, async 三种刷盘策略
 */
public class WalWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WalWriter.class);

    private final WalConfig config;
    private final Path dataDir;
    private final long segmentSizeBytes;
    private final String flushStrategy;

    private final List<WalSegment> segments;
    private WalSegment currentSegment;
    private final AtomicLong totalRecords = new AtomicLong(0);
    private final ScheduledExecutorService scheduler;

    private volatile boolean closed = false;
    private final Object writeLock = new Object();

    public WalWriter(WalConfig config) throws IOException {
        this.config = config;
        this.dataDir = Path.of(config.dataDir());
        this.segmentSizeBytes = config.segmentSizeMb() * 1024L * 1024L;
        this.flushStrategy = config.flushStrategy();
        this.segments = new ArrayList<>();

        // 创建数据目录
        Files.createDirectories(dataDir);

        // 加载或创建段文件
        loadOrInitializeSegments();

        // 启动异步刷盘（如果是 async 或 batch 模式）
        if ("async".equals(flushStrategy) || "batch".equals(flushStrategy)) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wal-flush-thread");
                t.setDaemon(true);
                return t;
            });
            this.scheduler.scheduleAtFixedRate(
                    this::flush,
                    config.batchFlushIntervalMs(),
                    config.batchFlushIntervalMs(),
                    TimeUnit.MILLISECONDS
            );
        } else {
            this.scheduler = null;
        }

        logger.info("WAL writer initialized with {} segments, flush strategy: {}",
                segments.size(), flushStrategy);
    }

    /**
     * 加载或初始化段文件
     */
    private void loadOrInitializeSegments() throws IOException {
        File[] files = dataDir.toFile().listFiles((dir, name) -> name.endsWith(WalConstants.FILE_EXTENSION));

        if (files != null && files.length > 0) {
            // 加载现有段
            for (File file : files) {
                int seq = WalSegment.parseSegmentSeq(file.getName());
                WalSegment segment = new WalSegment(file, seq, segmentSizeBytes, true);
                segments.add(segment);
                logger.debug("Loaded segment: {}", segment);
            }
            segments.sort(null);

            // 找到最后一个未满的段作为当前段
            if (!segments.isEmpty()) {
                WalSegment lastSegment = segments.get(segments.size() - 1);
                if (!lastSegment.isFull()) {
                    // 重新打开为可写
                    segments.remove(segments.size() - 1);
                    lastSegment.close();
                    currentSegment = new WalSegment(
                            lastSegment.getFile(),
                            lastSegment.getSegmentSeq(),
                            segmentSizeBytes,
                            false
                    );
                    currentSegment.markReadOnly(); // 需要重新设置为可写
                }
            }
        }

        // 如果没有当前段，创建新的
        if (currentSegment == null) {
            createNewSegment();
        }
    }

    /**
     * 创建新段
     */
    private void createNewSegment() throws IOException {
        int seq = segments.size();
        File file = dataDir.resolve(WalSegment.getFileName(seq)).toFile();
        currentSegment = new WalSegment(file, seq, segmentSizeBytes);
        logger.info("Created new WAL segment: {}", currentSegment.getFile().getName());
    }

    /**
     * 写入记录
     */
    public long append(String taskId, String bizKey, EventType eventType, long eventTime, byte[] payload) throws IOException {
        synchronized (writeLock) {
            if (closed) {
                throw new IllegalStateException("Writer is closed");
            }

            // 检查是否需要切换段
            if (currentSegment.isFull()) {
                rollover();
            }

            WalRecord record = WalRecord.create(
                    currentSegment.getSegmentSeq(),
                    currentSegment.getRecordSeq(),
                    taskId,
                    bizKey,
                    eventType,
                    eventTime,
                    payload
            );

            long position = currentSegment.write(record);
            totalRecords.incrementAndGet();

            // 根据策略决定是否刷盘
            if ("per_record".equals(flushStrategy)) {
                currentSegment.sync();
            } else if (config.syncOnWrite()) {
                currentSegment.sync();
            }

            return position;
        }
    }

    /**
     * 切换到新段
     */
    private void rollover() throws IOException {
        logger.info("Rolling over to new segment, current segment: {}", currentSegment.getFile().getName());

        // 写入结束标记
        currentSegment.writeEndMarker();
        currentSegment.markReadOnly();

        // 将当前段加入已完成列表
        segments.add(currentSegment);

        // 创建新段
        createNewSegment();
    }

    /**
     * 刷盘
     */
    public void flush() {
        synchronized (writeLock) {
            if (closed || currentSegment == null) {
                return;
            }
            try {
                currentSegment.sync();
                logger.debug("Flushed WAL segment: {}", currentSegment.getFile().getName());
            } catch (IOException e) {
                logger.error("Failed to flush WAL segment", e);
            }
        }
    }

    /**
     * 获取当前段
     */
    public WalSegment getCurrentSegment() {
        return currentSegment;
    }

    /**
     * 获取所有段（包括当前段）
     */
    public List<WalSegment> getAllSegments() {
        List<WalSegment> all = new ArrayList<>(segments);
        if (currentSegment != null) {
            all.add(currentSegment);
        }
        return all;
    }

    /**
     * 获取总记录数
     */
    public long getTotalRecords() {
        return totalRecords.get();
    }

    /**
     * 获取数据目录
     */
    public Path getDataDir() {
        return dataDir;
    }

    @Override
    public void close() {
        synchronized (writeLock) {
            if (closed) {
                return;
            }
            closed = true;
        }

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 最后一次刷盘
        if (currentSegment != null) {
            try {
                currentSegment.sync();
                currentSegment.close();
            } catch (IOException e) {
                logger.error("Failed to close current segment", e);
            }
        }

        // 关闭所有段
        for (WalSegment segment : segments) {
            segment.close();
        }

        logger.info("WAL writer closed, total records: {}", totalRecords.get());
    }
}
