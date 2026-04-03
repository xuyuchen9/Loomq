package com.loomq.wal;

/**
 * WAL 常量定义
 */
public final class WalConstants {
    private WalConstants() {}

    /**
     * WAL 文件魔数
     */
    public static final int MAGIC = 0x4C4F4F4D; // "LOOM"

    /**
     * 当前记录版本
     */
    public static final int RECORD_VERSION = 1;

    /**
     * 文件扩展名
     */
    public static final String FILE_EXTENSION = ".wal";

    /**
     * 段结束标记
     */
    public static final int SEGMENT_END_MARKER = 0xFFFFFFFF;

    /**
     * Header 大小: magic(4) + recordVersion(2) + recordType(1) + segmentSeq(4) + recordSeq(8) + taskIdLen(2) + bizKeyLen(2) + eventTime(8) + payloadLen(4) = 35 bytes
     */
    public static final int HEADER_SIZE = 35;

    /**
     * Checksum 大小
     */
    public static final int CHECKSUM_SIZE = 4;

    /**
     * 最大 taskId 长度
     */
    public static final int MAX_TASK_ID_LENGTH = 64;

    /**
     * 最大 bizKey 长度
     */
    public static final int MAX_BIZ_KEY_LENGTH = 128;

    /**
     * 最大 payload 长度 (1MB)
     */
    public static final int MAX_PAYLOAD_LENGTH = 1024 * 1024;
}
