package com.loomq.scheduler.v2;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 时间桶调度器 (V0.2 核心创新)
 *
 * 第一性原理推导：
 * 1. 延时调度的本质：在时间 T 执行任务 X
 * 2. V0.1 问题：每个任务独立调度 (Thread.sleep)，无法批量优化
 * 3. V0.2 方案：时间聚合，把同一秒的任务放入同一个桶
 *
 * 设计原理：
 * - 时间桶：Map<秒级时间戳, 优先队列(按毫秒排序)>
 * - 桶内排序：同一秒内的任务按毫秒时间排序
 * - 分批调度：每批次限制取出数量，避免执行风暴
 *
 * 收益：
 * - 消除唤醒风暴
 * - 支持批量优化
 * - 控制调度节奏
 * - 毫秒级调度精度
 */
public class TimeBucketScheduler {

    private static final Logger logger = LoggerFactory.getLogger(TimeBucketScheduler.class);

    // 时间桶：key = 秒级时间戳，value = 优先队列（按 triggerTime 排序）
    private final ConcurrentSkipListMap<Long, PriorityQueue<Task>> timeBuckets;

    // 待执行队列（有界）
    private final BlockingQueue<Task> readyQueue;

    // 调度线程
    private final ScheduledExecutorService scheduler;

    // 执行线程池
    private final ExecutorService executor;

    // 任务回调
    private final TaskDispatcher dispatcher;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 统计
    private final AtomicLong totalScheduled = new AtomicLong(0);
    private final AtomicLong totalDispatched = new AtomicLong(0);

    // 配置
    private final int readyQueueCapacity;
    private final int bucketGranularityMs;  // 桶粒度：1000ms = 1秒
    private final int maxDispatchPerBatch;  // 每批次最大分发数量

    public TimeBucketScheduler(TaskDispatcher dispatcher, int readyQueueCapacity) {
        this(dispatcher, readyQueueCapacity, 1000, 1000);
    }

    public TimeBucketScheduler(TaskDispatcher dispatcher, int readyQueueCapacity,
                               int bucketGranularityMs, int maxDispatchPerBatch) {
        this.dispatcher = dispatcher;
        this.readyQueueCapacity = readyQueueCapacity;
        this.bucketGranularityMs = bucketGranularityMs;
        this.maxDispatchPerBatch = maxDispatchPerBatch;

        this.timeBuckets = new ConcurrentSkipListMap<>();
        this.readyQueue = new ArrayBlockingQueue<>(readyQueueCapacity);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bucket-scheduler");
            t.setDaemon(true);
            return t;
        });

        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        // 启动桶扫描线程（每 100ms 检查一次）
        scheduler.scheduleAtFixedRate(this::processBuckets, 100, 100, TimeUnit.MILLISECONDS);

        // 启动执行线程
        scheduler.submit(this::dispatchLoop);

        logger.info("TimeBucketScheduler started, bucketGranularity={}ms, readyQueueCapacity={}, maxDispatchPerBatch={}",
                bucketGranularityMs, readyQueueCapacity, maxDispatchPerBatch);
    }

    /**
     * 停止调度器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        scheduler.shutdown();
        executor.shutdown();

        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("TimeBucketScheduler stopped, scheduled={}, dispatched={}",
                totalScheduled.get(), totalDispatched.get());
    }

    /**
     * 调度任务
     * 核心逻辑：将任务放入对应的时间桶（桶内按触发时间排序）
     */
    public boolean schedule(Task task) {
        if (!running.get()) {
            return false;
        }

        // 计算桶时间（向下取整到桶粒度）
        long bucketTime = alignToBucket(task.getTriggerTime());

        // 放入桶（优先队列，按 triggerTime 排序）
        timeBuckets.computeIfAbsent(bucketTime, k ->
                new PriorityQueue<>((a, b) -> Long.compare(a.getTriggerTime(), b.getTriggerTime()))
        ).offer(task);
        totalScheduled.incrementAndGet();

        logger.debug("Task {} scheduled to bucket {}, triggerTime={}",
                task.getTaskId(), bucketTime, task.getTriggerTime());
        return true;
    }

    /**
     * 取消任务
     */
    public boolean cancel(String taskId) {
        // 遍历所有桶查找并移除
        for (Map.Entry<Long, PriorityQueue<Task>> entry : timeBuckets.entrySet()) {
            PriorityQueue<Task> bucket = entry.getValue();
            if (bucket.removeIf(t -> t.getTaskId().equals(taskId))) {
                logger.debug("Task {} cancelled from bucket {}", taskId, entry.getKey());
                return true;
            }
        }

        // 也检查 ready 队列
        return readyQueue.removeIf(t -> t.getTaskId().equals(taskId));
    }

    /**
     * 重新调度
     */
    public boolean reschedule(Task task) {
        cancel(task.getTaskId());
        return schedule(task);
    }

    /**
     * 处理到期的时间桶
     * 核心逻辑：每 100ms 检查是否有到期的桶，按时间顺序取出任务
     */
    private void processBuckets() {
        if (!running.get()) {
            return;
        }

        long now = System.currentTimeMillis();
        long currentBucket = alignToBucket(now);

        // 取出所有到期的桶
        List<Task> dueTasks = new ArrayList<>();

        while (!timeBuckets.isEmpty()) {
            Map.Entry<Long, PriorityQueue<Task>> first = timeBuckets.firstEntry();
            if (first == null || first.getKey() > currentBucket) {
                break;
            }

            // 移除桶
            PriorityQueue<Task> bucket = timeBuckets.remove(first.getKey());

            // 按时间顺序取出到期的任务（限制每批数量）
            if (bucket != null) {
                while (!bucket.isEmpty() && dueTasks.size() < maxDispatchPerBatch) {
                    Task task = bucket.poll();
                    if (task != null && task.getTriggerTime() <= now) {
                        dueTasks.add(task);
                    } else if (task != null) {
                        // 任务还未到时间，放回桶中
                        bucket.offer(task);
                        break;  // 优先队列有序，后面的任务更晚
                    }
                }

                // 如果桶中还有任务，重新放回
                if (!bucket.isEmpty()) {
                    timeBuckets.put(first.getKey(), bucket);
                }
            }
        }

        if (!dueTasks.isEmpty()) {
            logger.debug("Processing {} due tasks from buckets (sorted by time)", dueTasks.size());

            // 放入 ready 队列
            for (Task task : dueTasks) {
                if (!readyQueue.offer(task)) {
                    // 队列满，记录警告
                    logger.warn("Ready queue full, task {} dropped", task.getTaskId());
                }
            }
        }
    }

    /**
     * 执行循环
     * 核心逻辑：从 ready 队列取任务，批量执行
     */
    private void dispatchLoop() {
        while (running.get()) {
            try {
                // 阻塞等待任务
                Task task = readyQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                // 检查状态
                if (task.getStatus().isTerminal()) {
                    logger.debug("Task {} already terminal, skip", task.getTaskId());
                    continue;
                }

                // 原子状态转换
                if (!task.tryStartExecution()) {
                    logger.debug("Task {} cannot start execution, status={}",
                            task.getTaskId(), task.getStatus());
                    continue;
                }

                // 提交执行
                executor.submit(() -> {
                    try {
                        dispatcher.dispatch(task);
                        totalDispatched.incrementAndGet();
                    } catch (Exception e) {
                        logger.error("Dispatch task {} failed", task.getTaskId(), e);
                    }
                });

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 将时间对齐到桶边界
     */
    private long alignToBucket(long timestamp) {
        return (timestamp / bucketGranularityMs) * bucketGranularityMs;
    }

    // ========== 统计接口 ==========

    public int getBucketCount() {
        return timeBuckets.size();
    }

    public int getReadyQueueSize() {
        return readyQueue.size();
    }

    public long getTotalScheduled() {
        return totalScheduled.get();
    }

    public long getTotalDispatched() {
        return totalDispatched.get();
    }

    public SchedulerStats getStats() {
        return new SchedulerStats(
                timeBuckets.size(),
                readyQueue.size(),
                totalScheduled.get(),
                totalDispatched.get(),
                running.get()
        );
    }

    public record SchedulerStats(
            int bucketCount,
            int readyQueueSize,
            long totalScheduled,
            long totalDispatched,
            boolean running
    ) {}
}
