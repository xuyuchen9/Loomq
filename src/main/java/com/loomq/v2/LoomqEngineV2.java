package com.loomq.v2;

import com.loomq.config.WalConfig;
import com.loomq.dispatcher.v2.DispatchLimiter;
import com.loomq.entity.Task;
import com.loomq.entity.v2.TaskLifecycle;
import com.loomq.scheduler.v2.TaskDispatcher;
import com.loomq.scheduler.v2.TimeBucketScheduler;
import com.loomq.wal.v2.AsyncWalWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Loomq V2 引擎
 *
 * 第一性原理推导的最终实现：
 *
 * 1. 调度层：时间桶 + 批量唤醒
 *    - 消除唤醒风暴
 *    - 控制调度节奏
 *
 * 2. 持久化层：异步 WAL + Group Commit
 *    - 消除全局锁
 *    - IO 与业务解耦
 *
 * 3. 执行层：限流器 + 背压
 *    - 防止 webhook 洪峰
 *    - 保护下游服务
 *
 * 4. 生命周期：完整状态机
 *    - 状态一致性保证
 *    - 可追踪的状态转换
 */
public class LoomqEngineV2 implements TaskDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(LoomqEngineV2.class);

    // 核心组件
    private final TimeBucketScheduler scheduler;
    private final AsyncWalWriter walWriter;
    private final DispatchLimiter dispatcher;
    private final WebhookExecutor webhookExecutor;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 配置
    private final LoomqConfigV2 config;

    // 监控服务
    private final MonitoringServiceV2 monitoring;

    // 指标刷新调度器
    private ScheduledExecutorService metricsScheduler;

    public LoomqEngineV2(LoomqConfigV2 config) throws IOException {
        this.config = config;

        // 创建组件
        this.webhookExecutor = new WebhookExecutor(config);

        this.dispatcher = new DispatchLimiter(
                this,  // delegate to self
                config.maxConcurrency(),
                config.dispatchQueueCapacity(),
                config.dispatchRatePerSecond()  // 速率限制
        );

        // 每批次最大分发数量：如果设置了速率限制则用速率，否则默认 1000
        int maxDispatchPerBatch = config.dispatchRatePerSecond() > 0
                ? config.dispatchRatePerSecond()
                : 1000;

        this.scheduler = new TimeBucketScheduler(
                this.dispatcher,
                config.readyQueueCapacity(),
                config.bucketGranularityMs(),
                maxDispatchPerBatch
        );

        this.walWriter = new AsyncWalWriter(config.toWalConfig());

        // 初始化监控服务
        this.monitoring = MonitoringServiceV2.getInstance();
        this.monitoring.setWalDataDir(config.walDataDir());
    }

    /**
     * 启动引擎
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        walWriter.start();
        scheduler.start();
        dispatcher.start();

        // 启动监控服务
        monitoring.start();

        // 启动指标刷新（每秒刷新一次）
        metricsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-refresh");
            t.setDaemon(true);
            return t;
        });
        metricsScheduler.scheduleAtFixedRate(this::refreshMetrics, 1, 1, TimeUnit.SECONDS);

        logger.info("LoomqEngineV2 started");
        logger.info("  - maxConcurrency: {}", config.maxConcurrency());
        logger.info("  - dispatchRatePerSecond: {}", config.dispatchRatePerSecond());
        logger.info("  - readyQueueCapacity: {}", config.readyQueueCapacity());
        logger.info("  - dispatchQueueCapacity: {}", config.dispatchQueueCapacity());
        logger.info("  - monitoring: enabled");
    }

    /**
     * 刷新指标到监控服务
     */
    private void refreshMetrics() {
        try {
            // 刷新调度器指标
            var schedulerStats = scheduler.getStats();
            monitoring.updateSchedulerStats(
                    schedulerStats.bucketCount(),
                    schedulerStats.readyQueueSize()
            );

            // 刷新分发器指标
            var dispatcherStats = dispatcher.getStats();
            monitoring.updateDispatcherStats(
                    dispatcherStats.queueSize(),
                    config.dispatchQueueCapacity(),
                    dispatcherStats.availablePermits(),
                    dispatcherStats.maxConcurrency()
            );

            // 刷新 WAL 指标
            var walStats = walWriter.getStats();
            monitoring.updateWalStats(
                    walStats.ringBufferSize(),
                    walStats.ringBufferCapacity(),
                    walStats.totalEvents()
            );
        } catch (Exception e) {
            logger.debug("Refresh metrics error: {}", e.getMessage());
        }
    }

    /**
     * 停止引擎
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // 停止指标刷新
        if (metricsScheduler != null) {
            metricsScheduler.shutdown();
        }

        // 停止监控服务
        monitoring.stop();

        scheduler.stop();
        dispatcher.stop();

        try {
            walWriter.close();
        } catch (Exception e) {
            logger.error("Close WAL writer failed", e);
        }

        logger.info("LoomqEngineV2 stopped");
    }

    /**
     * 创建任务（使用默认 ACK 级别）
     */
    public CreateResult createTask(Task task) {
        return createTask(task, config.defaultAckLevel());
    }

    /**
     * 创建任务（指定 ACK 级别）
     *
     * @param task 任务
     * @param ackLevel ACK 级别
     *                 - ASYNC: publish 后返回，RPO < 100ms
     *                 - DURABLE: fsync 后返回，RPO = 0
     */
    public CreateResult createTask(Task task, LoomqConfigV2.AckLevel ackLevel) {
        if (!running.get()) {
            return CreateResult.notRunning();
        }

        try {
            // 1. 写入 WAL
            long sequence = walWriter.append(
                    task.getTaskId(),
                    task.getBizKey(),
                    com.loomq.entity.EventType.CREATE,
                    System.currentTimeMillis(),
                    new byte[0]  // 空 payload
            );

            // 2. 如果是 DURABLE 级别，等待 fsync
            if (ackLevel == LoomqConfigV2.AckLevel.DURABLE) {
                boolean durable = walWriter.awaitDurable(sequence, 5000);
                if (!durable) {
                    logger.error("Task {} WAL sync timeout", task.getTaskId());
                    return CreateResult.walError("WAL sync timeout");
                }
            }

            // 3. 调度任务
            boolean scheduled = scheduler.schedule(task);

            if (scheduled) {
                // 记录指标
                monitoring.incrementTasksCreated();
                return CreateResult.success(task.getTaskId(), ackLevel);
            } else {
                return CreateResult.scheduleFailed();
            }

        } catch (IOException e) {
            logger.error("Create task {} failed", task.getTaskId(), e);
            return CreateResult.walError(e.getMessage());
        }
    }

    /**
     * 取消任务
     */
    public CancelResult cancelTask(String taskId) {
        if (!running.get()) {
            return CancelResult.notRunning();
        }

        boolean cancelled = scheduler.cancel(taskId);

        if (cancelled) {
            // 写入 CANCEL 事件
            try {
                walWriter.append(
                        taskId,
                        null,
                        com.loomq.entity.EventType.CANCEL,
                        System.currentTimeMillis(),
                        new byte[0]
                );
            } catch (IOException e) {
                logger.error("Write CANCEL event failed for task {}", taskId, e);
            }

            return CancelResult.success();
        } else {
            return CancelResult.notFound();
        }
    }

    /**
     * 执行任务（由 DispatchLimiter 调用）
     */
    @Override
    public void dispatch(Task task) {
        if (!running.get()) {
            logger.debug("Engine not running, skip dispatch: {}", task.getTaskId());
            return;
        }

        // 记录唤醒延迟（计划时间 → 开始执行）
        long wakeLatency = System.currentTimeMillis() - task.getTriggerTime();
        monitoring.recordWakeLatency(Math.max(0, wakeLatency));

        // 记录 webhook 请求
        monitoring.incrementWebhookRequests();

        // 执行 webhook
        long webhookStart = System.currentTimeMillis();
        WebhookExecutor.ExecuteResult result = webhookExecutor.execute(task);
        long webhookLatency = System.currentTimeMillis() - webhookStart;

        // 记录 webhook 延迟
        monitoring.recordWebhookLatency(webhookLatency);

        switch (result.status()) {
            case SUCCESS:
                // 转换到 ACKED
                task.transitionToAcked();
                // 写入 ACK 事件
                writeAckEvent(task, result.durationMs());
                // 记录指标
                monitoring.incrementTasksAckSuccess();
                // 记录总延迟
                monitoring.recordTotalLatency(System.currentTimeMillis() - task.getTriggerTime());
                break;

            case RETRY:
                // 转换到 RETRY
                if (task.transitionToRetry(config.maxRetry())) {
                    // 写入 RETRY 事件
                    writeRetryEvent(task);
                    // 重新调度
                    scheduler.reschedule(task);
                    // 记录重试指标
                    monitoring.incrementTasksRetry();
                }
                break;

            case DEAD:
                // 转换到 DEAD
                task.transitionToDead(result.error());
                writeDeadEvent(task);
                // 记录失败指标
                monitoring.incrementTasksFailedTerminal();
                break;
        }
    }

    private void writeAckEvent(Task task, long durationMs) {
        try {
            walWriter.append(
                    task.getTaskId(),
                    task.getBizKey(),
                    com.loomq.entity.EventType.ACK,
                    System.currentTimeMillis(),
                    ("{\"duration_ms\":" + durationMs + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8)
            );
        } catch (IOException e) {
            logger.error("Write ACK event failed", e);
        }
    }

    private void writeRetryEvent(Task task) {
        try {
            walWriter.append(
                    task.getTaskId(),
                    task.getBizKey(),
                    com.loomq.entity.EventType.RETRY,
                    System.currentTimeMillis(),
                    ("{\"retry_count\":" + task.getRetryCount() + "}").getBytes()
            );
        } catch (IOException e) {
            logger.error("Write RETRY event failed", e);
        }
    }

    private void writeDeadEvent(Task task) {
        try {
            walWriter.append(
                    task.getTaskId(),
                    task.getBizKey(),
                    com.loomq.entity.EventType.FAIL,
                    System.currentTimeMillis(),
                    ("{\"error\":\"" + task.getLastError() + "\"}").getBytes()
            );
        } catch (IOException e) {
            logger.error("Write DEAD event failed", e);
        }
    }

    // ========== 统计接口 ==========

    public EngineStats getStats() {
        return new EngineStats(
                scheduler.getStats(),
                dispatcher.getStats(),
                walWriter.getStats(),
                running.get()
        );
    }

    public record EngineStats(
            TimeBucketScheduler.SchedulerStats scheduler,
            DispatchLimiter.LimiterStats dispatcher,
            AsyncWalWriter.WalStats wal,
            boolean running
    ) {}

    // ========== 结果类型 ==========

    public record CreateResult(
            boolean ok,
            String taskId,
            String ackLevel,
            String error
    ) {
        public static CreateResult success(String taskId, LoomqConfigV2.AckLevel ackLevel) {
            return new CreateResult(true, taskId, ackLevel.name(), null);
        }

        public static CreateResult notRunning() {
            return new CreateResult(false, null, null, "Engine not running");
        }

        public static CreateResult scheduleFailed() {
            return new CreateResult(false, null, null, "Schedule failed");
        }

        public static CreateResult walError(String error) {
            return new CreateResult(false, null, null, "WAL error: " + error);
        }
    }

    public record CancelResult(
            boolean ok,
            String error
    ) {
        public static CancelResult success() {
            return new CancelResult(true, null);
        }

        public static CancelResult notRunning() {
            return new CancelResult(false, "Engine not running");
        }

        public static CancelResult notFound() {
            return new CancelResult(false, "Task not found or not cancellable");
        }
    }
}
