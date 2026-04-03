package com.loomq.entity;

/**
 * 任务状态枚举
 *
 * 状态流转：
 * PENDING -> SCHEDULED -> DUE -> DISPATCHING -> ACKED
 *                                   -> RETRY_WAIT -> DISPATCHING
 *                                   -> FAILED_TERMINAL
 * PENDING/SCHEDULED/RETRY_WAIT -> CANCELLED
 * SCHEDULED/DUE -> EXPIRED
 */
public enum TaskStatus {
    /**
     * 已创建，待调度
     */
    PENDING,

    /**
     * 已入调度队列
     */
    SCHEDULED,

    /**
     * 到期，待执行
     */
    DUE,

    /**
     * 执行中
     */
    DISPATCHING,

    /**
     * 重试等待中
     */
    RETRY_WAIT,

    /**
     * 成功确认（终态）
     */
    ACKED,

    /**
     * 已取消（终态）
     */
    CANCELLED,

    /**
     * 已过期（终态）
     */
    EXPIRED,

    /**
     * 终态失败（终态）
     */
    FAILED_TERMINAL;

    /**
     * 判断是否为终态
     */
    public boolean isTerminal() {
        return this == ACKED || this == CANCELLED || this == EXPIRED || this == FAILED_TERMINAL;
    }

    /**
     * 判断任务是否可取消
     */
    public boolean isCancellable() {
        return this == PENDING || this == SCHEDULED || this == RETRY_WAIT;
    }

    /**
     * 判断任务是否可修改
     */
    public boolean isModifiable() {
        return this == PENDING || this == SCHEDULED || this == RETRY_WAIT;
    }
}
