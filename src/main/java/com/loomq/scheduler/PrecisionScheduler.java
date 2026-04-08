package com.loomq.scheduler;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 精确调度器 (V0.3+ 核心实现) - 使用 ScheduledExecutorService
 *
 * 设计原理 (基于设计文档 §6)：
 * 1. 使用最小堆结构（PriorityBlockingQueue）按 triggerTime 排序
 * 2. 调度线程精确睡眠到最早任务到期
 * 3. 新任务插入时，如果比当前等待的任务更早，唤醒调度线程
 *
 * 性能特性：
 * - 插入：O(logN)
 * - 取最小：O(1)
 * - 取消：O(1)（通过辅助 Map）
 *
 * 与 TimeBucketScheduler 对比：
 * | 维度    | TimeBucket        | PrecisionScheduler |
 * |--------|-------------------|-------------------|
 * | 精度    | 受 bucket 限制     | 精确时间           |
 * | 扫描    | O(N bucket)       | O(1)              |
 * | 插入    | O(1)              | O(logN)           |
 * | 空转    | 每 100ms 轮询      | 无空转            |
 *
 * @author loomq
 * @since v0.3+
 */
public class PrecisionScheduler {

    private static final Logger logger = LoggerFactory.getLogger(PrecisionScheduler.class);

    // 默认队列容量
    public static final int DEFAULT_QUEUE_CAPACITY = 100000;

    // 任务队列（最小堆，按 triggerTime 排序）
    private final PriorityBlockingQueue<ScheduledTask> taskQueue;

    // 任务索引（用于 O(1) 取消）
    private final ConcurrentHashMap<String, ScheduledTask> taskIndex;

    // 分发器
    private final TaskDispatcher dispatcher;

    // 执行线程池（虚拟线程）
    private final ExecutorService executor;

    // 调度线程池（使用 ScheduledExecutorService 统一管理）
    private final ScheduledExecutorService scheduler;

    // 当前调度的 Future（用于取消当前等待任务）
    private volatile ScheduledFuture<?> currentScheduleFuture;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 当前等待的任务截止时间（用于判断是否需要重新调度）
    private final AtomicLong currentWaitDeadline = new AtomicLong(Long.MAX_VALUE);

    // 统计
    private final Stats stats = new Stats();

    // 配置
    private final int queueCapacity;

    /**
     * 创建精确调度器
     *
     * @param dispatcher 任务分发器
     * @param queueCapacity 队列容量
     */
    public PrecisionScheduler(TaskDispatcher dispatcher, int queueCapacity) {
        this.dispatcher = dispatcher;
        this.queueCapacity = queueCapacity;

        // 创建最小堆队列（按 triggerTime 升序）
        this.taskQueue = new PriorityBlockingQueue<>(
                queueCapacity,
                Comparator.comparingLong(ScheduledTask::getTriggerTime)
        );

        this.taskIndex = new ConcurrentHashMap<>();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        // 使用单线程 ScheduledExecutorService 替代独立 Thread
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "precision-scheduler");
            t.setDaemon(true);
            return t;
        });

        logger.info("PrecisionScheduler created, queueCapacity={}", queueCapacity);
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        // 启动调度循环
        scheduleNext();

        logger.info("PrecisionScheduler started");
    }

    /**
     * 停止调度器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // 取消当前调度
        ScheduledFuture<?> future = currentScheduleFuture;
        if (future != null) {
            future.cancel(false);
        }

        scheduler.shutdown();
        executor.shutdown();

        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("PrecisionScheduler stopped, stats={}", stats);
    }

    /**
     * 调度任务
     * 核心逻辑：插入队列，如果比当前等待的任务更早，唤醒调度线程
     */
    public boolean schedule(Task task) {
        if (!running.get()) {
            return false;
        }
        if (task == null || task.getTaskId() == null) {
            return false;
        }

        String taskId = task.getTaskId();

        // 将任务状态设置为 SCHEDULED（如果当前是 PENDING）
        if (task.getStatus() == TaskStatus.PENDING) {
            task.transitionToScheduled();
        }

        // 创建调度任务包装
        ScheduledTask scheduledTask = new ScheduledTask(task);

        // 记录到索引（用于 O(1) 取消）
        ScheduledTask existing = taskIndex.putIfAbsent(taskId, scheduledTask);
        if (existing != null) {
            // 任务已存在
            logger.warn("Task {} already scheduled", taskId);
            return false;
        }

        // 插入队列
        taskQueue.offer(scheduledTask);
        stats.recordSchedule();

        // 检查是否需要重新调度
        long triggerTime = task.getWakeTime();
        long currentDeadline = currentWaitDeadline.get();

        if (triggerTime < currentDeadline) {
            // 新任务比当前等待的任务更早，需要重新调度
            logger.debug("Task {} is earlier than current deadline, reschedule (triggerTime={}, currentDeadline={})",
                    taskId, triggerTime, currentDeadline);
            rescheduleNext();
        }

        logger.debug("Task {} scheduled, triggerTime={}, delayMs={}",
                taskId, triggerTime, triggerTime - System.currentTimeMillis());

        return true;
    }

    /**
     * 取消任务（O(1)）
     */
    public boolean cancel(String taskId) {
        if (taskId == null) {
            return false;
        }

        // 从索引中移除
        ScheduledTask scheduledTask = taskIndex.remove(taskId);
        if (scheduledTask == null) {
            return false;
        }

        // 标记为已取消（惰性删除，不立即从队列移除）
        scheduledTask.cancel();
        stats.recordCancel();

        logger.debug("Task {} cancelled", taskId);
        return true;
    }

    /**
     * 重新调度
     */
    public boolean reschedule(Task task) {
        cancel(task.getTaskId());
        return schedule(task);
    }

    /**
     * 立即触发任务
     */
    public boolean fireNow(String taskId) {
        ScheduledTask scheduledTask = taskIndex.get(taskId);
        if (scheduledTask == null) {
            return false;
        }

        // 标记原任务为取消（惰性删除）
        scheduledTask.cancel();

        // 创建新任务并立即执行
        Task task = scheduledTask.getTask();
        ScheduledTask newTask = new ScheduledTask(task);
        newTask.updateTriggerTime(System.currentTimeMillis());

        // 更新索引
        taskIndex.put(taskId, newTask);

        // 加入队列
        taskQueue.offer(newTask);

        // 重新调度
        rescheduleNext();

        logger.debug("Task {} fired now", taskId);
        return true;
    }

    /**
     * 调度下一个任务（核心算法）
     *
     * 逻辑：
     * 1. 查看队列中最早到期的任务
     * 2. 如果任务已到期，立即执行
     * 3. 如果任务未到期，schedule 到其到期时间
     */
    private void scheduleNext() {
        if (!running.get()) {
            return;
        }

        try {
            // 获取最早到期的任务
            ScheduledTask scheduledTask = taskQueue.peek();

            if (scheduledTask == null) {
                // 队列为空，设置无限等待，等待新任务唤醒
                currentWaitDeadline.set(Long.MAX_VALUE);
                return;
            }

            // 检查是否已取消（惰性删除）
            if (scheduledTask.isCancelled()) {
                taskQueue.poll();  // 移除已取消任务
                scheduleNext();    // 递归处理下一个
                return;
            }

            long now = System.currentTimeMillis();
            long triggerTime = scheduledTask.getTriggerTime();
            long delayMs = triggerTime - now;

            if (delayMs > 0) {
                // 任务尚未到期，精确调度
                currentWaitDeadline.set(triggerTime);
                currentScheduleFuture = scheduler.schedule(this::onTaskExpire, delayMs, TimeUnit.MILLISECONDS);
            } else {
                // 任务已到期，立即执行
                onTaskExpire();
            }

        } catch (Exception e) {
            logger.error("Schedule next error", e);
            // 出错后重试
            currentScheduleFuture = scheduler.schedule(this::scheduleNext, 100, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 重新调度（当新任务比当前等待任务更早时调用）
     */
    private void rescheduleNext() {
        // 取消当前调度
        ScheduledFuture<?> future = currentScheduleFuture;
        if (future != null) {
            future.cancel(false);
        }

        // 立即重新调度
        scheduleNext();
    }

    /**
     * 任务到期处理
     */
    private void onTaskExpire() {
        if (!running.get()) {
            return;
        }

        try {
            // 获取并移除到期任务
            ScheduledTask scheduledTask = taskQueue.poll();
            if (scheduledTask == null || scheduledTask.isCancelled()) {
                scheduleNext();
                return;
            }

            // 从索引中移除
            taskIndex.remove(scheduledTask.getTaskId());

            // 检查状态
            Task task = scheduledTask.getTask();
            if (task.getStatus().isTerminal()) {
                logger.debug("Task {} already terminal, skip", task.getTaskId());
                scheduleNext();
                return;
            }

            // 原子状态转换：READY -> RUNNING
            if (!task.transitionToReady()) {
                logger.debug("Task {} cannot transition to READY, status={}",
                        task.getTaskId(), task.getStatus());
                scheduleNext();
                return;
            }
            if (!task.transitionToRunning()) {
                logger.debug("Task {} cannot transition to RUNNING, status={}",
                        task.getTaskId(), task.getStatus());
                scheduleNext();
                return;
            }

            // 提交执行
            stats.recordExpire();
            dispatchTask(task);

            // 继续调度下一个任务
            scheduleNext();

        } catch (Exception e) {
            logger.error("Task expire error", e);
            scheduleNext();
        }
    }

    /**
     * 分发任务（虚拟线程执行）
     */
    private void dispatchTask(Task task) {
        executor.submit(() -> {
            try {
                dispatcher.dispatch(task);
                stats.recordDispatch();
            } catch (Exception e) {
                logger.error("Dispatch task {} failed", task.getTaskId(), e);
            }
        });
    }

    // ========== 统计接口 ==========

    public int getQueueSize() {
        return taskQueue.size();
    }

    public int getPendingTaskCount() {
        return taskIndex.size();
    }

    public Stats getStats() {
        return stats;
    }

    public SchedulerStats getSchedulerStats() {
        return new SchedulerStats(
                taskQueue.size(),
                stats.getScheduledCount(),
                stats.getExpiredCount(),
                running.get()
        );
    }

    public record SchedulerStats(
            int queueSize,
            long totalScheduled,
            long totalExpired,
            boolean running
    ) {}

    // ========== 内部类 ==========

    /**
     * 调度任务包装（支持动态修改 triggerTime 和取消标记）
     */
    private static class ScheduledTask {
        private final Task task;
        private volatile long triggerTime;
        private volatile boolean cancelled = false;

        ScheduledTask(Task task) {
            this.task = task;
            this.triggerTime = task.getWakeTime();
        }

        Task getTask() {
            return task;
        }

        long getTriggerTime() {
            return triggerTime;
        }

        void updateTriggerTime(long newTriggerTime) {
            this.triggerTime = newTriggerTime;
        }

        void cancel() {
            this.cancelled = true;
        }

        boolean isCancelled() {
            return cancelled;
        }

        String getTaskId() {
            return task.getTaskId();
        }
    }

    /**
     * 统计信息
     */
    public static class Stats {
        private final AtomicLong scheduledCount = new AtomicLong(0);
        private final AtomicLong cancelledCount = new AtomicLong(0);
        private final AtomicLong expiredCount = new AtomicLong(0);
        private final AtomicLong dispatchedCount = new AtomicLong(0);

        void recordSchedule() { scheduledCount.incrementAndGet(); }
        void recordCancel() { cancelledCount.incrementAndGet(); }
        void recordExpire() { expiredCount.incrementAndGet(); }
        void recordDispatch() { dispatchedCount.incrementAndGet(); }

        public long getScheduledCount() { return scheduledCount.get(); }
        public long getCancelledCount() { return cancelledCount.get(); }
        public long getExpiredCount() { return expiredCount.get(); }
        public long getDispatchedCount() { return dispatchedCount.get(); }

        @Override
        public String toString() {
            return String.format("Stats{scheduled=%d, cancelled=%d, expired=%d, dispatched=%d}",
                    getScheduledCount(), getCancelledCount(), getExpiredCount(), getDispatchedCount());
        }
    }
}
