package com.loomq.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static com.loomq.wal.WalConstants.*;

/**
 * WAL 段文件
 * 每个段文件有固定大小上限，写满后切换到新段
 */
public class WalSegment implements Comparable<WalSegment>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WalSegment.class);

    private final File file;
    private final int segmentSeq;
    private final long maxSize;
    private RandomAccessFile raf;
    private FileChannel channel;
    private long currentPosition;
    private long recordSeq;
    private boolean readOnly;

    public WalSegment(File file, int segmentSeq, long maxSize) throws IOException {
        this.file = file;
        this.segmentSeq = segmentSeq;
        this.maxSize = maxSize;
        this.readOnly = false;
        open();
    }

    public WalSegment(File file, int segmentSeq, long maxSize, boolean readOnly) throws IOException {
        this.file = file;
        this.segmentSeq = segmentSeq;
        this.maxSize = maxSize;
        this.readOnly = readOnly;
        open();
    }

    /**
     * 打开段文件
     */
    private void open() throws IOException {
        String mode = readOnly ? "r" : "rw";
        raf = new RandomAccessFile(file, mode);
        channel = raf.getChannel();

        if (file.exists() && file.length() > 0) {
            currentPosition = file.length();
            // 尝试读取最后的 recordSeq
            recoverRecordSeq();
        } else {
            currentPosition = 0;
            recordSeq = 0;
        }
    }

    /**
     * 恢复 recordSeq
     * 增强版：处理 kill -9 后的不完整记录
     */
    private void recoverRecordSeq() throws IOException {
        long pos = 0;
        recordSeq = 0;

        channel.position(0);
        ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

        while (channel.position() < currentPosition) {
            long recordStartPos = channel.position();

            try {
                headerBuffer.clear();
                int read = channel.read(headerBuffer);
                if (read < HEADER_SIZE) {
                    // 不完整的 header，截断
                    logger.warn("Incomplete header at position {}, truncating", recordStartPos);
                    break;
                }

                headerBuffer.flip();
                int magic = headerBuffer.getInt();
                if (magic != MAGIC) {
                    // 无效 magic，可能是段结束或损坏
                    if (magic == SEGMENT_END_MARKER) {
                        logger.debug("Found segment end marker at position {}", recordStartPos);
                    } else {
                        logger.warn("Invalid magic {} at position {}, truncating", magic, recordStartPos);
                    }
                    break;
                }

                // 读取记录字段
                short version = headerBuffer.getShort();
                byte type = headerBuffer.get();
                int segSeq = headerBuffer.getInt();
                long seq = headerBuffer.getLong();
                short taskIdLen = headerBuffer.getShort();
                short bizKeyLen = headerBuffer.getShort();
                long eventTime = headerBuffer.getLong();
                int payloadLen = headerBuffer.getInt();

                // 验证字段合理性
                if (taskIdLen < 0 || taskIdLen > 256 ||
                    bizKeyLen < 0 || bizKeyLen > 512 ||
                    payloadLen < 0 || payloadLen > 10 * 1024 * 1024) { // 最大 10MB payload
                    logger.warn("Invalid record fields at position {}, truncating: taskIdLen={}, bizKeyLen={}, payloadLen={}",
                            recordStartPos, taskIdLen, bizKeyLen, payloadLen);
                    break;
                }

                // 计算记录大小
                long newPos = channel.position() + taskIdLen + bizKeyLen + payloadLen + CHECKSUM_SIZE;
                if (newPos > currentPosition) {
                    // 不完整的记录，截断
                    logger.warn("Incomplete record at position {}, expected end={}, actual size={}",
                            recordStartPos, newPos, currentPosition);
                    break;
                }

                // 跳过数据部分
                channel.position(newPos);

                recordSeq = seq + 1;
                pos = newPos;

            } catch (java.nio.BufferUnderflowException e) {
                logger.warn("Buffer underflow at position {}, truncating: {}", recordStartPos, e.getMessage());
                break;
            } catch (Exception e) {
                logger.warn("Error reading record at position {}, truncating: {}", recordStartPos, e.getMessage());
                break;
            }
        }

        currentPosition = pos;
        logger.info("Recovered segment {} with {} records, recordSeq {}", segmentSeq, recordSeq, recordSeq);
    }

    /**
     * 写入记录
     */
    public synchronized long write(WalRecord record) throws IOException {
        if (readOnly) {
            throw new IllegalStateException("Segment is read-only");
        }

        record.setSegmentSeq(segmentSeq);
        record.setRecordSeq(recordSeq);

        byte[] data = record.encode();
        long position = currentPosition;

        channel.position(position);
        channel.write(ByteBuffer.wrap(data));
        currentPosition += data.length;
        recordSeq++;

        return position;
    }

    /**
     * 同步到磁盘
     */
    public synchronized void sync() throws IOException {
        if (!readOnly) {
            channel.force(false);
        }
    }

    /**
     * 判断是否已满
     */
    public boolean isFull() {
        return currentPosition >= maxSize;
    }

    /**
     * 获取当前大小
     */
    public long size() {
        return currentPosition;
    }

    /**
     * 获取剩余空间
     */
    public long remaining() {
        return maxSize - currentPosition;
    }

    /**
     * 标记为只读并关闭写入
     */
    public void markReadOnly() {
        this.readOnly = true;
    }

    /**
     * 写入段结束标记
     */
    public synchronized void writeEndMarker() throws IOException {
        if (readOnly) {
            return;
        }
        ByteBuffer marker = ByteBuffer.allocate(4);
        marker.putInt(SEGMENT_END_MARKER);
        marker.flip();
        channel.write(marker);
        currentPosition += 4;
        sync();
    }

    /**
     * 获取文件路径
     */
    public File getFile() {
        return file;
    }

    /**
     * 获取段序号
     */
    public int getSegmentSeq() {
        return segmentSeq;
    }

    /**
     * 获取当前记录序号
     */
    public long getRecordSeq() {
        return recordSeq;
    }

    /**
     * 获取 FileChannel 用于读取
     */
    public FileChannel getChannel() {
        return channel;
    }

    /**
     * 获取文件名
     */
    public static String getFileName(int segmentSeq) {
        return String.format("%08d%s", segmentSeq, FILE_EXTENSION);
    }

    /**
     * 从文件名解析段序号
     */
    public static int parseSegmentSeq(String fileName) {
        if (!fileName.endsWith(FILE_EXTENSION)) {
            throw new IllegalArgumentException("Invalid WAL file name: " + fileName);
        }
        String seqStr = fileName.substring(0, fileName.length() - FILE_EXTENSION.length());
        return Integer.parseInt(seqStr, 10);
    }

    @Override
    public int compareTo(WalSegment other) {
        return Integer.compare(this.segmentSeq, other.segmentSeq);
    }

    @Override
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (raf != null) {
                raf.close();
            }
        } catch (IOException e) {
            logger.warn("Failed to close segment file: {}", file, e);
        }
    }

    @Override
    public String toString() {
        return "WalSegment{" +
                "file=" + file.getName() +
                ", segmentSeq=" + segmentSeq +
                ", size=" + currentPosition +
                ", maxSize=" + maxSize +
                ", readOnly=" + readOnly +
                '}';
    }
}
