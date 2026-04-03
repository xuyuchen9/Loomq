package com.loomq.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.loomq.wal.WalConstants.*;

/**
 * WAL 读取器
 * 用于恢复时顺序读取所有记录
 */
public class WalReader implements Iterable<WalRecord>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WalReader.class);

    private final List<WalSegment> segments;
    private final boolean safeMode;

    public WalReader(List<WalSegment> segments, boolean safeMode) {
        this.segments = new ArrayList<>(segments);
        this.segments.sort(null); // 按序号排序
        this.safeMode = safeMode;
    }

    /**
     * 读取所有记录
     */
    public List<WalRecord> readAll() throws IOException {
        List<WalRecord> records = new ArrayList<>();
        for (WalRecord record : this) {
            records.add(record);
        }
        return records;
    }

    /**
     * 获取记录迭代器
     */
    @Override
    public Iterator<WalRecord> iterator() {
        return new WalRecordIterator();
    }

    /**
     * 获取段数量
     */
    public int getSegmentCount() {
        return segments.size();
    }

    @Override
    public void close() {
        // WalReader does not own the segments, so it should not close them.
        // The segments are owned by WalWriter/WalEngine and will be closed when they are closed.
        // This is a no-op to avoid accidentally closing segments that are still in use.
    }

    /**
     * WAL 记录迭代器
     */
    private class WalRecordIterator implements Iterator<WalRecord> {
        private int currentSegmentIndex = 0;
        private FileChannel currentChannel;
        private long currentPosition = 0;
        private WalRecord nextRecord;
        private boolean finished = false;

        public WalRecordIterator() {
            if (!segments.isEmpty()) {
                currentChannel = segments.get(0).getChannel();
            }
        }

        @Override
        public boolean hasNext() {
            if (finished) {
                return false;
            }
            if (nextRecord != null) {
                return true;
            }
            nextRecord = readNext();
            if (nextRecord == null) {
                finished = true;
            }
            return nextRecord != null;
        }

        @Override
        public WalRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            WalRecord record = nextRecord;
            nextRecord = null;
            return record;
        }

        private WalRecord readNext() {
            while (currentSegmentIndex < segments.size()) {
                try {
                    WalRecord record = readFromCurrentSegment();
                    if (record != null) {
                        return record;
                    }
                    // 当前段已读完，切换到下一个段
                    currentSegmentIndex++;
                    if (currentSegmentIndex < segments.size()) {
                        currentChannel = segments.get(currentSegmentIndex).getChannel();
                        currentPosition = 0;
                    }
                } catch (IOException e) {
                    if (safeMode) {
                        logger.error("Error reading segment {}, stopping iteration", currentSegmentIndex, e);
                        finished = true;
                        return null;
                    } else {
                        logger.warn("Error reading segment {}, skipping to next segment", currentSegmentIndex, e);
                        currentSegmentIndex++;
                        if (currentSegmentIndex < segments.size()) {
                            currentChannel = segments.get(currentSegmentIndex).getChannel();
                            currentPosition = 0;
                        }
                    }
                }
            }
            return null;
        }

        private WalRecord readFromCurrentSegment() throws IOException {
            if (currentChannel == null) {
                return null;
            }

            long fileSize = currentChannel.size();
            if (currentPosition >= fileSize) {
                return null;
            }

            // 读取 header
            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
            currentChannel.position(currentPosition);
            int headerRead = currentChannel.read(headerBuffer);

            if (headerRead < HEADER_SIZE) {
                logger.debug("Incomplete header at position {} in segment {}", currentPosition, currentSegmentIndex);
                return null;
            }

            headerBuffer.flip();

            // 解析 header
            int magic = headerBuffer.getInt();
            if (magic == SEGMENT_END_MARKER) {
                // 段结束标记
                logger.debug("Reached end marker in segment {}", currentSegmentIndex);
                return null;
            }
            if (magic != MAGIC) {
                throw new IOException("Invalid magic number at position " + currentPosition);
            }

            short recordVersion = headerBuffer.getShort();
            byte recordType = headerBuffer.get();
            int segmentSeq = headerBuffer.getInt();
            long recordSeq = headerBuffer.getLong();
            short taskIdLen = headerBuffer.getShort();
            short bizKeyLen = headerBuffer.getShort();
            long eventTime = headerBuffer.getLong();
            int payloadLen = headerBuffer.getInt();

            // 验证长度
            if (taskIdLen < 0 || taskIdLen > MAX_TASK_ID_LENGTH ||
                bizKeyLen < 0 || bizKeyLen > MAX_BIZ_KEY_LENGTH ||
                payloadLen < 0 || payloadLen > MAX_PAYLOAD_LENGTH) {
                throw new IOException("Invalid record lengths at position " + currentPosition);
            }

            // 计算总大小
            int totalSize = HEADER_SIZE + taskIdLen + bizKeyLen + payloadLen + CHECKSUM_SIZE;
            if (currentPosition + totalSize > fileSize) {
                throw new IOException("Incomplete record at position " + currentPosition);
            }

            // 读取完整记录
            ByteBuffer recordBuffer = ByteBuffer.allocate(totalSize);
            currentChannel.position(currentPosition);
            int totalRead = currentChannel.read(recordBuffer);

            if (totalRead < totalSize) {
                throw new IOException("Incomplete record at position " + currentPosition);
            }

            // 解码记录
            WalRecord record = WalRecord.decode(recordBuffer.array());
            currentPosition += totalSize;

            return record;
        }
    }

    /**
     * 从指定段序号开始读取
     */
    public static WalReader fromSegment(List<WalSegment> allSegments, int startSegmentSeq, boolean safeMode) {
        List<WalSegment> filtered = new ArrayList<>();
        for (WalSegment segment : allSegments) {
            if (segment.getSegmentSeq() >= startSegmentSeq) {
                filtered.add(segment);
            }
        }
        return new WalReader(filtered, safeMode);
    }
}
