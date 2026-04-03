package com.loomq.entity.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 任务生命周期管理器
 *
 * 第一性原理推导：
 * 1. 状态转换的本质：任务在不同阶段的变化
 * 2. 核心要求：状态一致性，无中间状态
 * 3. 实现方式：原子操作 + 状态机
 *
 * 设计原理：
 * - 所有状态转换通过 CAS 操作
 * - 无效转换被拒绝
 * - 状态变化可追踪
 */
public class TaskLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(TaskLifecycle.class);

    private final String taskId;
    private final AtomicReference<TaskStatusV2> status;
    private final Object transitionLock = new Object();

    // 执行计数
    private volatile int attemptCount = 0;
    private volatile int retryCount = 0;

    // 时间戳
    private volatile long createTime;
    private volatile long readyTime;
    private volatile long dispatchTime;
    private volatile long ackTime;
    private volatile long lastErrorTime;
    private volatile String lastError;

    public TaskLifecycle(String taskId) {
        this.taskId = taskId;
        this.status = new AtomicReference<>(TaskStatusV2.PENDING);
        this.createTime = System.currentTimeMillis();
    }

    /**
     * 获取当前状态
     */
    public TaskStatusV2 getStatus() {
        return status.get();
    }

    // ========== 状态转换方法（原子操作）==========

    /**
     * 转换到 READY 状态
     * 条件：当前状态为 PENDING
     */
    public boolean transitionToReady() {
        synchronized (transitionLock) {
            TaskStatusV2 current = status.get();
            if (current == TaskStatusV2.PENDING || current == TaskStatusV2.RETRY) {
                status.set(TaskStatusV2.READY);
                readyTime = System.currentTimeMillis();
                logger.debug("Task {} transitioned to READY from {}", taskId, current);
                return true;
            }
            logger.debug("Task {} cannot transition to READY from {}", taskId, current);
            return false;
        }
    }

    /**
     * 转换到 DISPATCHING 状态
     * 条件：当前状态为 READY
     */
    public boolean transitionToDispatching() {
        synchronized (transitionLock) {
            TaskStatusV2 current = status.get();
            if (current == TaskStatusV2.READY) {
                status.set(TaskStatusV2.DISPATCHING);
                dispatchTime = System.currentTimeMillis();
                attemptCount++;
                logger.debug("Task {} transitioned to DISPATCHING (attempt {})", taskId, attemptCount);
                return true;
            }
            logger.debug("Task {} cannot transition to DISPATCHING from {}", taskId, current);
            return false;
        }
    }

    /**
     * 转换到 ACKED 状态
     * 条件：当前状态为 DISPATCHING
     */
    public boolean transitionToAcked() {
        synchronized (transitionLock) {
            TaskStatusV2 current = status.get();
            if (current == TaskStatusV2.DISPATCHING) {
                status.set(TaskStatusV2.ACKED);
                ackTime = System.currentTimeMillis();
                logger.info("Task {} ACKED after {} attempts", taskId, attemptCount);
                return true;
            }
            logger.debug("Task {} cannot transition to ACKED from {}", taskId, current);
            return false;
        }
    }

    /**
     * 转换到 RETRY 状态
     * 条件：当前状态为 DISPATCHING，且重试次数未耗尽
     */
    public boolean transitionToRetry(int maxRetry) {
        synchronized (transitionLock) {
            TaskStatusV2 current = status.get();
            if (current == TaskStatusV2.DISPATCHING) {
                retryCount++;
                if (retryCount >= maxRetry) {
                    // 重试耗尽，进入 DEAD
                    status.set(TaskStatusV2.DEAD);
                    logger.warn("Task {} transitioned to DEAD after {} retries", taskId, retryCount);
                    return true;
                }
                status.set(TaskStatusV2.RETRY);
                logger.info("Task {} transitioned to RETRY (attempt {})", taskId, retryCount);
                return true;
            }
            logger.debug("Task {} cannot transition to RETRY from {}", taskId, current);
            return false;
        }
    }

    /**
     * 转换到 DEAD 状态
     * 条件：任意非终态
     */
    public boolean transitionToDead(String error) {
        synchronized (transitionLock) {
            TaskStatusV2 current = status.get();
            if (!current.isTerminal()) {
                status.set(TaskStatusV2.DEAD);
                lastError = error;
                lastErrorTime = System.currentTimeMillis();
                logger.warn("Task {} transitioned to DEAD: {}", taskId, error);
                return true;
            }
            return false;
        }
    }

    /**
     * 取消任务
     * 条件：当前状态可取消
     */
    public boolean cancel() {
        synchronized (transitionLock) {
            TaskStatusV2 current = status.get();
            if (current.isCancellable()) {
                status.set(TaskStatusV2.CANCELLED);
                logger.info("Task {} CANCELLED from {}", taskId, current);
                return true;
            }
            logger.debug("Task {} cannot be cancelled from {}", taskId, current);
            return false;
        }
    }

    /**
     * 过期任务
     * 条件：当前状态非终态
     */
    public boolean expire() {
        synchronized (transitionLock) {
            TaskStatusV2 current = status.get();
            if (!current.isTerminal()) {
                status.set(TaskStatusV2.EXPIRED);
                logger.info("Task {} EXPIRED from {}", taskId, current);
                return true;
            }
            return false;
        }
    }

    // ========== 获取器 ==========

    public int getAttemptCount() {
        return attemptCount;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getReadyTime() {
        return readyTime;
    }

    public long getDispatchTime() {
        return dispatchTime;
    }

    public long getAckTime() {
        return ackTime;
    }

    public String getLastError() {
        return lastError;
    }

    /**
     * 获取状态摘要
     */
    public LifecycleSummary getSummary() {
        return new LifecycleSummary(
                taskId,
                status.get(),
                attemptCount,
                retryCount,
                createTime,
                readyTime,
                dispatchTime,
                ackTime,
                lastError
        );
    }

    public record LifecycleSummary(
            String taskId,
            TaskStatusV2 status,
            int attemptCount,
            int retryCount,
            long createTime,
            long readyTime,
            long dispatchTime,
            long ackTime,
            String lastError
    ) {}
}
