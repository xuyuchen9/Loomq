package com.loomq.entity;

/**
 * 事件类型枚举
 */
public enum EventType {
    /**
     * 任务创建
     */
    CREATE(1),

    /**
     * 任务调度（进入调度队列）
     */
    SCHEDULE(2),

    /**
     * 任务到期
     */
    DUE(3),

    /**
     * 开始分发执行
     */
    DISPATCH(4),

    /**
     * 成功确认
     */
    ACK(5),

    /**
     * 进入重试等待
     */
    RETRY(6),

    /**
     * 任务取消
     */
    CANCEL(7),

    /**
     * 任务修改
     */
    MODIFY(8),

    /**
     * 终态失败
     */
    FAIL(9),

    /**
     * 任务过期
     */
    EXPIRE(10),

    /**
     * 立即触发
     */
    FIRE_NOW(11);

    private final int code;

    EventType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static EventType fromCode(int code) {
        for (EventType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown event type code: " + code);
    }
}
