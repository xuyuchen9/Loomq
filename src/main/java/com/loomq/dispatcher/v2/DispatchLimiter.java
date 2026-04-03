package com.loomq.dispatcher.v2;

import com.loomq.entity.Task;
import com.loomq.scheduler.v2.TaskDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 执行层限流器 (V0.2 风暴保护)
 *
 * 第一性原理推导：
 * 1. webhook 执行的本质：HTTP 请求
 * 2. 风暴风险：大量任务同时执行，打垮下游
 * 3. 控制手段：限制并发数 + 控制速率
 *
 * 设计原理：
 * - Semaphore 控制最大并发数
 * - RateLimiter 控制执行速率（令牌桶）
 * - 有界队列防止内存溢出
 * - 背压机制：队列满时拒绝
 *
 * 收益：
 * - 防止 webhook 洪峰
 * - 保护下游服务
 * - 系统可预测退化
 */
public class DispatchLimiter implements TaskDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(DispatchLimiter.class);

    // 并发控制
    private final Semaphore semaphore;

    // 速率控制（令牌桶算法）
    private final long ratePerSecond;
    private final AtomicLong lastRefillTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong availableTokens = new AtomicLong(0);

    // 执行队列（有界）
    private final BlockingQueue<Task> executeQueue;

    // 执行线程池
    private final ExecutorService executor;

    // 实际的分发器
    private final TaskDispatcher delegate;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 统计
    private final AtomicLong totalSubmitted = new AtomicLong(0);
    private final AtomicLong totalExecuted = new AtomicLong(0);
    private final AtomicLong totalRejected = new AtomicLong(0);
    private final AtomicLong totalRateLimited = new AtomicLong(0);

    // 配置
    private final int maxConcurrency;
    private final int queueCapacity;

    /**
     * 构造函数
     *
     * @param delegate 实际执行器
     * @param maxConcurrency 最大并发数
     * @param queueCapacity 队列容量
     */
    public DispatchLimiter(TaskDispatcher delegate, int maxConcurrency, int queueCapacity) {
        this(delegate, maxConcurrency, queueCapacity, 0);
    }

    /**
     * 构造函数（带速率限制）
     *
     * @param delegate 实际执行器
     * @param maxConcurrency 最大并发数
     * @param queueCapacity 队列容量
     * @param ratePerSecond 每秒最大执行数（0=不限速）
     */
    public DispatchLimiter(TaskDispatcher delegate, int maxConcurrency, int queueCapacity, int ratePerSecond) {
        this.delegate = delegate;
        this.maxConcurrency = maxConcurrency;
        this.queueCapacity = queueCapacity;
        this.ratePerSecond = ratePerSecond;

        this.semaphore = new Semaphore(maxConcurrency);
        this.executeQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        if (ratePerSecond > 0) {
            this.availableTokens.set(ratePerSecond);
        }
    }

    /**
     * 启动限流器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        // 启动执行线程
        Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "dispatch-limiter");
            t.setDaemon(true);
            return t;
        }).submit(this::executeLoop);

        logger.info("DispatchLimiter started, maxConcurrency={}, queueCapacity={}",
                maxConcurrency, queueCapacity);
    }

    /**
     * 停止限流器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("DispatchLimiter stopped, submitted={}, executed={}, rejected={}",
                totalSubmitted.get(), totalExecuted.get(), totalRejected.get());
    }

    /**
     * 提交任务
     * 核心逻辑：放入队列，由执行线程消费
     */
    public DispatchResult submit(Task task) {
        totalSubmitted.incrementAndGet();

        if (!running.get()) {
            totalRejected.incrementAndGet();
            return DispatchResult.REJECTED_NOT_RUNNING;
        }

        // 非阻塞放入队列
        if (!executeQueue.offer(task)) {
            totalRejected.incrementAndGet();
            logger.warn("Execute queue full, task {} rejected", task.getTaskId());
            return DispatchResult.REJECTED_QUEUE_FULL;
        }

        return DispatchResult.ACCEPTED;
    }

    /**
     * 执行循环
     */
    private void executeLoop() {
        while (running.get()) {
            try {
                // 从队列取任务
                Task task = executeQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                // 速率控制（令牌桶）
                if (ratePerSecond > 0 && !tryAcquireRate()) {
                    // 速率限制，放回队列尾部
                    if (executeQueue.offer(task)) {
                        totalRateLimited.incrementAndGet();
                        // 短暂等待
                        Thread.sleep(1);
                    } else {
                        totalRejected.incrementAndGet();
                    }
                    continue;
                }

                // 获取许可（限制并发）
                semaphore.acquire();

                // 提交执行
                executor.submit(() -> {
                    try {
                        delegate.dispatch(task);
                        totalExecuted.incrementAndGet();
                    } catch (Exception e) {
                        logger.error("Dispatch task {} failed", task.getTaskId(), e);
                    } finally {
                        semaphore.release();
                    }
                });

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 尝试获取速率令牌（令牌桶算法）
     */
    private boolean tryAcquireRate() {
        long now = System.currentTimeMillis();
        long lastRefill = lastRefillTime.get();

        // 每秒补充令牌
        if (now - lastRefill >= 1000) {
            long newTokens = ratePerSecond;
            if (lastRefillTime.compareAndSet(lastRefill, now)) {
                availableTokens.set(newTokens);
            }
        }

        // 尝试获取令牌
        while (true) {
            long current = availableTokens.get();
            if (current <= 0) {
                return false;
            }
            if (availableTokens.compareAndSet(current, current - 1)) {
                return true;
            }
        }
    }

    /**
     * 直接分发（绕过限流，用于高优先级任务）
     */
    @Override
    public void dispatch(Task task) {
        delegate.dispatch(task);
    }

    // ========== 统计接口 ==========

    public int getQueueSize() {
        return executeQueue.size();
    }

    public int getAvailablePermits() {
        return semaphore.availablePermits();
    }

    public long getTotalSubmitted() {
        return totalSubmitted.get();
    }

    public long getTotalExecuted() {
        return totalExecuted.get();
    }

    public long getTotalRejected() {
        return totalRejected.get();
    }

    public LimiterStats getStats() {
        return new LimiterStats(
                executeQueue.size(),
                semaphore.availablePermits(),
                maxConcurrency,
                (int) ratePerSecond,
                totalSubmitted.get(),
                totalExecuted.get(),
                totalRejected.get(),
                totalRateLimited.get(),
                running.get()
        );
    }

    public record LimiterStats(
            int queueSize,
            int availablePermits,
            int maxConcurrency,
            int ratePerSecond,
            long totalSubmitted,
            long totalExecuted,
            long totalRejected,
            long totalRateLimited,
            boolean running
    ) {}

    /**
     * 分发结果
     */
    public enum DispatchResult {
        ACCEPTED,           // 已接受
        REJECTED_QUEUE_FULL, // 队列满
        REJECTED_NOT_RUNNING // 未运行
    }
}
