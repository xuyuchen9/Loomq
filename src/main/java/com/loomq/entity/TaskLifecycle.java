package com.loomq.entity;

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
 * - 无效转换被拒绝并记录
 * - 状态变化可追踪
 *
 * 合法流转规则：
 * PENDING → SCHEDULED | CANCELLED
 * SCHEDULED → READY | CANCELLED | EXPIRED
 * READY → RUNNING | CANCELLED
 * RUNNING → SUCCESS | RETRY_WAIT | FAILED
 * RETRY_WAIT → SCHEDULED | FAILED | DEAD_LETTER
 */
public class TaskLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(TaskLifecycle.class);

    private final String taskId;
    private final AtomicReference<TaskStatus> status;
    private final Object transitionLock = new Object();

    // 执行计数
    private volatile int attemptCount = 0;
    private volatile int retryCount = 0;

    // 时间戳
    private volatile long createTime;
    private volatile long scheduledTime;
    private volatile long readyTime;
    private volatile long executionStartTime;
    private volatile long completionTime;
    private volatile long lastErrorTime;
    private volatile String lastError;

    public TaskLifecycle(String taskId) {
        this.taskId = taskId;
        this.status = new AtomicReference<>(TaskStatus.PENDING);
        this.createTime = System.currentTimeMillis();
    }

    /**
     * 获取当前状态
     */
    public TaskStatus getStatus() {
        return status.get();
    }

    // ========== 状态转换方法（原子操作）==========

    /**
     * PENDING/RETRY_WAIT → SCHEDULED
     * 任务进入调度系统
     */
    public boolean transitionToScheduled() {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current == TaskStatus.PENDING || current == TaskStatus.RETRY_WAIT) {
                status.set(TaskStatus.SCHEDULED);
                scheduledTime = System.currentTimeMillis();
                logger.debug("Task {} transitioned to SCHEDULED from {}", taskId, current);
                return true;
            }
            logInvalidTransition(TaskStatus.SCHEDULED, current);
            return false;
        }
    }

    /**
     * SCHEDULED → READY
     * 任务到期，准备执行
     */
    public boolean transitionToReady() {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current == TaskStatus.SCHEDULED || current == TaskStatus.RETRY_WAIT) {
                status.set(TaskStatus.READY);
                readyTime = System.currentTimeMillis();
                logger.debug("Task {} transitioned to READY from {}", taskId, current);
                return true;
            }
            logInvalidTransition(TaskStatus.READY, current);
            return false;
        }
    }

    /**
     * READY → RUNNING
     * 开始执行任务
     */
    public boolean transitionToRunning() {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current == TaskStatus.READY) {
                status.set(TaskStatus.RUNNING);
                executionStartTime = System.currentTimeMillis();
                attemptCount++;
                logger.debug("Task {} transitioned to RUNNING (attempt {})", taskId, attemptCount);
                return true;
            }
            logInvalidTransition(TaskStatus.RUNNING, current);
            return false;
        }
    }

    /**
     * RUNNING → SUCCESS
     * 执行成功
     */
    public boolean transitionToSuccess() {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current == TaskStatus.RUNNING) {
                status.set(TaskStatus.SUCCESS);
                completionTime = System.currentTimeMillis();
                logger.debug("Task {} SUCCESS after {} attempts", taskId, attemptCount);
                return true;
            }
            logInvalidTransition(TaskStatus.SUCCESS, current);
            return false;
        }
    }

    /**
     * RUNNING → RETRY_WAIT
     * 执行失败，等待重试
     *
     * @param maxRetry 最大重试次数
     * @return 是否成功转换到 RETRY_WAIT 或 DEAD_LETTER
     */
    public boolean transitionToRetryWait(int maxRetry) {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current == TaskStatus.RUNNING) {
                retryCount++;
                if (retryCount >= maxRetry) {
                    // 重试耗尽，进入死信
                    status.set(TaskStatus.DEAD_LETTER);
                    completionTime = System.currentTimeMillis();
                    logger.debug("Task {} entered DEAD_LETTER after {} retries", taskId, retryCount);
                    return true;
                }
                status.set(TaskStatus.RETRY_WAIT);
                logger.debug("Task {} transitioned to RETRY_WAIT (retry {}/{})", taskId, retryCount, maxRetry);
                return true;
            }
            logInvalidTransition(TaskStatus.RETRY_WAIT, current);
            return false;
        }
    }

    /**
     * RUNNING → FAILED
     * 执行最终失败（不可重试）
     */
    public boolean transitionToFailed(String error) {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current == TaskStatus.RUNNING) {
                status.set(TaskStatus.FAILED);
                lastError = error;
                lastErrorTime = System.currentTimeMillis();
                completionTime = System.currentTimeMillis();
                logger.warn("Task {} FAILED: {}", taskId, error);
                return true;
            }
            logInvalidTransition(TaskStatus.FAILED, current);
            return false;
        }
    }

    /**
     * RETRY_WAIT → DEAD_LETTER
     * 重试耗尽，进入死信
     */
    public boolean transitionToDeadLetter(String error) {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current == TaskStatus.RETRY_WAIT || current == TaskStatus.RUNNING) {
                status.set(TaskStatus.DEAD_LETTER);
                lastError = error;
                lastErrorTime = System.currentTimeMillis();
                completionTime = System.currentTimeMillis();
                logger.warn("Task {} entered DEAD_LETTER: {}", taskId, error);
                return true;
            }
            logInvalidTransition(TaskStatus.DEAD_LETTER, current);
            return false;
        }
    }

    /**
     * 取消任务
     * PENDING/SCHEDULED/READY/RETRY_WAIT → CANCELLED
     */
    public boolean cancel() {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current.isCancellable()) {
                status.set(TaskStatus.CANCELLED);
                completionTime = System.currentTimeMillis();
                logger.debug("Task {} CANCELLED from {}", taskId, current);
                return true;
            }
            logger.debug("Task {} cannot be cancelled from {}", taskId, current);
            return false;
        }
    }

    /**
     * 过期任务
     * SCHEDULED/READY → EXPIRED
     */
    public boolean expire() {
        synchronized (transitionLock) {
            TaskStatus current = status.get();
            if (current == TaskStatus.SCHEDULED || current == TaskStatus.READY) {
                status.set(TaskStatus.EXPIRED);
                completionTime = System.currentTimeMillis();
                logger.info("Task {} EXPIRED from {}", taskId, current);
                return true;
            }
            return false;
        }
    }

    /**
     * 强制设置状态（仅用于恢复）
     */
    public void forceSetStatus(TaskStatus newStatus) {
        synchronized (transitionLock) {
            status.set(newStatus);
            logger.debug("Task {} force set to {}", taskId, newStatus);
        }
    }

    // ========== 验证方法 ==========

    /**
     * 检查是否可以转换到目标状态
     */
    public boolean canTransitionTo(TaskStatus target) {
        TaskStatus current = status.get();
        return isValidTransition(current, target);
    }

    /**
     * 验证状态转换是否合法
     */
    public static boolean isValidTransition(TaskStatus from, TaskStatus to) {
        if (from == to) return false;
        if (from.isTerminal()) return false;

        return switch (from) {
            case PENDING -> to == TaskStatus.SCHEDULED || to == TaskStatus.CANCELLED;
            case SCHEDULED -> to == TaskStatus.READY || to == TaskStatus.CANCELLED || to == TaskStatus.EXPIRED;
            case READY -> to == TaskStatus.RUNNING || to == TaskStatus.CANCELLED;
            case RUNNING -> to == TaskStatus.SUCCESS || to == TaskStatus.RETRY_WAIT ||
                           to == TaskStatus.FAILED || to == TaskStatus.DEAD_LETTER;
            case RETRY_WAIT -> to == TaskStatus.SCHEDULED || to == TaskStatus.FAILED ||
                              to == TaskStatus.DEAD_LETTER || to == TaskStatus.CANCELLED;
            default -> false;
        };
    }

    private void logInvalidTransition(TaskStatus target, TaskStatus current) {
        logger.debug("Task {} cannot transition from {} to {}", taskId, current, target);
    }

    // ========== 获取器 ==========

    public String getTaskId() {
        return taskId;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getScheduledTime() {
        return scheduledTime;
    }

    public long getReadyTime() {
        return readyTime;
    }

    public long getExecutionStartTime() {
        return executionStartTime;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public long getLastErrorTime() {
        return lastErrorTime;
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
                scheduledTime,
                readyTime,
                executionStartTime,
                completionTime,
                lastError
        );
    }

    public record LifecycleSummary(
            String taskId,
            TaskStatus status,
            int attemptCount,
            int retryCount,
            long createTime,
            long scheduledTime,
            long readyTime,
            long executionStartTime,
            long completionTime,
            String lastError
    ) {}
}
