package com.loomq.entity.v2;

/**
 * 任务状态 V2（完整状态机）
 *
 * 状态流转：
 * PENDING → READY → DISPATCHING → ACKED
 *                 ↘ RETRY → READY (重试)
 *                 ↘ DEAD (死信)
 * PENDING/READY → CANCELLED
 *
 * 状态说明：
 * - PENDING: 已创建，等待调度
 * - READY: 到期，准备执行
 * - DISPATCHING: 正在执行
 * - ACKED: 执行成功（终态）
 * - RETRY: 重试中
 * - DEAD: 死信（终态，重试耗尽）
 * - CANCELLED: 已取消（终态）
 * - EXPIRED: 已过期（终态）
 */
public enum TaskStatusV2 {

    /**
     * 已创建，等待调度
     */
    PENDING,

    /**
     * 到期，准备执行
     */
    READY,

    /**
     * 正在执行
     */
    DISPATCHING,

    /**
     * 重试中
     */
    RETRY,

    /**
     * 执行成功（终态）
     */
    ACKED,

    /**
     * 死信（终态，重试耗尽）
     */
    DEAD,

    /**
     * 已取消（终态）
     */
    CANCELLED,

    /**
     * 已过期（终态）
     */
    EXPIRED;

    /**
     * 判断是否为终态
     */
    public boolean isTerminal() {
        return this == ACKED || this == DEAD || this == CANCELLED || this == EXPIRED;
    }

    /**
     * 判断是否可取消
     */
    public boolean isCancellable() {
        return this == PENDING || this == READY || this == RETRY;
    }

    /**
     * 判断是否可执行
     */
    public boolean isExecutable() {
        return this == READY || this == RETRY;
    }

    /**
     * 判断是否可重试
     */
    public boolean canRetry() {
        return this == DISPATCHING;
    }
}
