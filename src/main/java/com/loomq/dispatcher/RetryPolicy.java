package com.loomq.dispatcher;

import com.loomq.config.RetryConfig;
import com.loomq.entity.Task;

/**
 * 重试策略
 * 支持指数退避
 */
public class RetryPolicy {
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double multiplier;
    private final int defaultMaxRetry;

    public RetryPolicy(RetryConfig config) {
        this.initialDelayMs = config.initialDelayMs();
        this.maxDelayMs = config.maxDelayMs();
        this.multiplier = config.multiplier();
        this.defaultMaxRetry = config.defaultMaxRetry();
    }

    /**
     * 计算下次重试延迟
     */
    public long calculateDelay(Task task) {
        return calculateDelay(task.getRetryCount());
    }

    /**
     * 计算指定重试次数的延迟
     */
    public long calculateDelay(int retryCount) {
        if (retryCount <= 0) {
            return initialDelayMs;
        }

        long delay = initialDelayMs;
        for (int i = 0; i < retryCount; i++) {
            delay = (long) (delay * multiplier);
            if (delay >= maxDelayMs) {
                return maxDelayMs;
            }
        }
        return Math.min(delay, maxDelayMs);
    }

    /**
     * 计算下次触发时间
     */
    public long calculateNextTriggerTime(Task task) {
        long delay = calculateDelay(task.getRetryCount());
        return System.currentTimeMillis() + delay;
    }

    /**
     * 判断是否可以重试
     */
    public boolean canRetry(Task task) {
        return task.getRetryCount() < task.getMaxRetry() && !task.getStatus().isTerminal();
    }

    /**
     * 获取默认最大重试次数
     */
    public int getDefaultMaxRetry() {
        return defaultMaxRetry;
    }
}
