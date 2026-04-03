package com.loomq.wal;

import com.loomq.entity.EventType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static com.loomq.wal.WalConstants.*;

/**
 * WAL 记录
 *
 * 二进制格式：
 * +--------+--------+--------+----------+----------+-----------+
 * | magic  | record | record | segment  | record   | taskId    |
 * | (4B)   | version| type   | seq (4B) | seq (8B) | length(2B)|
 * +--------+--------+--------+----------+----------+-----------+
 * | taskId (变长) | bizKey length (2B) | bizKey (变长)       |
 * +---------------+--------------------+---------------------+
 * | event_time (8B) | payload_length (4B) | payload (变长)   |
 * +-----------------+---------------------+------------------+
 * | checksum (4B)                                                    |
 * +------------------------------------------------------------------+
 */
public class WalRecord {
    private int magic;
    private short recordVersion;
    private byte recordType;
    private int segmentSeq;
    private long recordSeq;
    private String taskId;
    private String bizKey;
    private long eventTime;
    private byte[] payload;
    private int checksum;

    public WalRecord() {
        this.magic = MAGIC;
        this.recordVersion = RECORD_VERSION;
    }

    // ========== Factory Methods ==========

    public static WalRecord create(int segmentSeq, long recordSeq, String taskId, String bizKey,
                                    EventType eventType, long eventTime, byte[] payload) {
        WalRecord record = new WalRecord();
        record.setSegmentSeq(segmentSeq);
        record.setRecordSeq(recordSeq);
        record.setTaskId(taskId);
        record.setBizKey(bizKey);
        record.setRecordType((byte) eventType.getCode());
        record.setEventTime(eventTime);
        record.setPayload(payload);
        return record;
    }

    // ========== Serialization ==========

    /**
     * 编码为字节数组
     */
    public byte[] encode() {
        Objects.requireNonNull(taskId, "taskId is required");
        Objects.requireNonNull(payload, "payload is required");

        byte[] taskIdBytes = taskId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] bizKeyBytes = bizKey != null ? bizKey.getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];

        if (taskIdBytes.length > MAX_TASK_ID_LENGTH) {
            throw new IllegalArgumentException("taskId too long: " + taskIdBytes.length);
        }
        if (bizKeyBytes.length > MAX_BIZ_KEY_LENGTH) {
            throw new IllegalArgumentException("bizKey too long: " + bizKeyBytes.length);
        }
        if (payload.length > MAX_PAYLOAD_LENGTH) {
            throw new IllegalArgumentException("payload too long: " + payload.length);
        }

        int totalSize = HEADER_SIZE + taskIdBytes.length + bizKeyBytes.length + payload.length + CHECKSUM_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Header
        buffer.putInt(magic);
        buffer.putShort(recordVersion);
        buffer.put(recordType);
        buffer.putInt(segmentSeq);
        buffer.putLong(recordSeq);
        buffer.putShort((short) taskIdBytes.length);
        buffer.putShort((short) bizKeyBytes.length);
        buffer.putLong(eventTime);
        buffer.putInt(payload.length);

        // Data
        buffer.put(taskIdBytes);
        buffer.put(bizKeyBytes);
        buffer.put(payload);

        // Checksum (CRC32 of everything before)
        int crc = calculateChecksum(buffer.array(), 0, buffer.position());
        buffer.putInt(crc);

        this.checksum = crc;
        return buffer.array();
    }

    /**
     * 从字节数组解码
     */
    public static WalRecord decode(byte[] data) {
        return decode(data, 0, data.length);
    }

    /**
     * 从字节数组解码
     */
    public static WalRecord decode(byte[] data, int offset, int length) {
        if (length < HEADER_SIZE + CHECKSUM_SIZE) {
            throw new IllegalArgumentException("Data too short for WAL record");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
        WalRecord record = new WalRecord();

        // Header
        record.magic = buffer.getInt();
        record.recordVersion = buffer.getShort();
        record.recordType = buffer.get();
        record.segmentSeq = buffer.getInt();
        record.recordSeq = buffer.getLong();

        short taskIdLen = buffer.getShort();
        short bizKeyLen = buffer.getShort();
        record.eventTime = buffer.getLong();
        int payloadLen = buffer.getInt();

        // Validate lengths
        if (taskIdLen < 0 || taskIdLen > MAX_TASK_ID_LENGTH) {
            throw new IllegalArgumentException("Invalid taskId length: " + taskIdLen);
        }
        if (bizKeyLen < 0 || bizKeyLen > MAX_BIZ_KEY_LENGTH) {
            throw new IllegalArgumentException("Invalid bizKey length: " + bizKeyLen);
        }
        if (payloadLen < 0 || payloadLen > MAX_PAYLOAD_LENGTH) {
            throw new IllegalArgumentException("Invalid payload length: " + payloadLen);
        }

        // Data
        byte[] taskIdBytes = new byte[taskIdLen];
        buffer.get(taskIdBytes);
        record.taskId = new String(taskIdBytes, java.nio.charset.StandardCharsets.UTF_8);

        byte[] bizKeyBytes = new byte[bizKeyLen];
        buffer.get(bizKeyBytes);
        record.bizKey = bizKeyLen > 0 ? new String(bizKeyBytes, java.nio.charset.StandardCharsets.UTF_8) : null;

        record.payload = new byte[payloadLen];
        buffer.get(record.payload);

        // Checksum
        record.checksum = buffer.getInt();

        // Verify checksum
        int calculatedCrc = calculateChecksum(data, offset, length - CHECKSUM_SIZE);
        if (calculatedCrc != record.checksum) {
            throw new IllegalArgumentException("Checksum mismatch: expected " + record.checksum + ", got " + calculatedCrc);
        }

        // Verify magic
        if (record.magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic: " + record.magic);
        }

        return record;
    }

    /**
     * 计算记录总大小
     */
    public int calculateTotalSize() {
        int taskIdLen = taskId != null ? taskId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length : 0;
        int bizKeyLen = bizKey != null ? bizKey.getBytes(java.nio.charset.StandardCharsets.UTF_8).length : 0;
        int payloadLen = payload != null ? payload.length : 0;
        return HEADER_SIZE + taskIdLen + bizKeyLen + payloadLen + CHECKSUM_SIZE;
    }

    /**
     * 计算校验和 (CRC32)
     */
    private static int calculateChecksum(byte[] data, int offset, int length) {
        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(data, offset, length);
        return (int) crc32.getValue();
    }

    // ========== Getters and Setters ==========

    public int getMagic() {
        return magic;
    }

    public short getRecordVersion() {
        return recordVersion;
    }

    public byte getRecordType() {
        return recordType;
    }

    public void setRecordType(byte recordType) {
        this.recordType = recordType;
    }

    public EventType getEventType() {
        return EventType.fromCode(recordType & 0xFF);
    }

    public int getSegmentSeq() {
        return segmentSeq;
    }

    public void setSegmentSeq(int segmentSeq) {
        this.segmentSeq = segmentSeq;
    }

    public long getRecordSeq() {
        return recordSeq;
    }

    public void setRecordSeq(long recordSeq) {
        this.recordSeq = recordSeq;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getBizKey() {
        return bizKey;
    }

    public void setBizKey(String bizKey) {
        this.bizKey = bizKey;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public int getChecksum() {
        return checksum;
    }

    @Override
    public String toString() {
        return "WalRecord{" +
                "segmentSeq=" + segmentSeq +
                ", recordSeq=" + recordSeq +
                ", taskId='" + taskId + '\'' +
                ", bizKey='" + bizKey + '\'' +
                ", eventType=" + getEventType() +
                ", eventTime=" + eventTime +
                ", payloadSize=" + (payload != null ? payload.length : 0) +
                '}';
    }
}
