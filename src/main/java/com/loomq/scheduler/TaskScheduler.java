package com.loomq.scheduler;

import com.loomq.common.IdGenerator;
import com.loomq.common.MetricsCollector;
import com.loomq.config.SchedulerConfig;
import com.loomq.dispatcher.WebhookDispatcher;
import com.loomq.entity.EventType;
import com.loomq.entity.Task;
import com.loomq.entity.TaskEvent;
import com.loomq.entity.TaskStatus;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 任务调度器
 *
 * 核心创新 (INNO-20240520-002):
 * 使用 Thread.sleep() 直接在虚拟线程上实现延时调度。
 * 每个任务 = 一个虚拟线程，该线程 sleep(delay) 后执行。
 * 休眠的虚拟线程本身就是延时队列。
 *
 * 设计原理：
 * - 虚拟线程在 sleep 时会 unmount，不再占用 carrier thread
 * - 休眠的虚拟线程只占用 ~几百字节 heap 内存
 * - 几百万个休眠的虚拟线程 = 完美的延时队列
 * - O(1) 极简实现，无需 DelayQueue、时间轮、ZSET 等复杂数据结构
 */
public class TaskScheduler {
    private static final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);

    private final WalEngine walEngine;
    private final TaskStore taskStore;
    private final WebhookDispatcher dispatcher;
    private final SchedulerConfig config;

    // 存储 task_id -> 虚拟线程 的映射，用于取消任务
    private final ConcurrentHashMap<String, Thread> taskThreads;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public TaskScheduler(WalEngine walEngine, TaskStore taskStore, WebhookDispatcher dispatcher, SchedulerConfig config) {
        this.walEngine = walEngine;
        this.taskStore = taskStore;
        this.dispatcher = dispatcher;
        this.config = config;
        this.taskThreads = new ConcurrentHashMap<>();
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        logger.info("Task scheduler started, using Thread.sleep() on virtual threads");
    }

    /**
     * 停止调度器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping task scheduler, interrupting {} sleeping threads", taskThreads.size());

        // 中断所有休眠线程
        for (Thread vt : taskThreads.values()) {
            vt.interrupt();
        }

        // 等待所有线程结束
        for (Thread vt : taskThreads.values()) {
            try {
                vt.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        taskThreads.clear();
        logger.info("Task scheduler stopped");
    }

    /**
     * 调度任务
     * 核心创新：直接在虚拟线程上 Thread.sleep(delay)
     */
    public void schedule(Task task) {
        if (!running.get()) {
            throw new IllegalStateException("Scheduler is not running");
        }

        // 更新状态为 SCHEDULED
        if (task.getStatus() == TaskStatus.PENDING) {
            TaskStatus oldStatus = task.getStatus();
            task.setStatus(TaskStatus.SCHEDULED);
            taskStore.updateStatus(task, oldStatus);
        }

        long delayMs = task.getTriggerTime() - System.currentTimeMillis();

        // 创建虚拟线程，直接 sleep
        Thread vt = Thread.ofVirtual()
                .name("task-" + task.getTaskId())
                .start(() -> {
                    try {
                        // 核心创新：直接 sleep
                        if (delayMs > 0) {
                            logger.debug("Task {} sleeping for {} ms", task.getTaskId(), delayMs);
                            Thread.sleep(Duration.ofMillis(delayMs));
                        }

                        // 醒来后检查调度器是否仍在运行
                        if (!running.get()) {
                            logger.debug("Scheduler stopped, skip dispatch: {}", task.getTaskId());
                            return;
                        }

                        // 检查任务是否被取消
                        if (task.getStatus().isTerminal()) {
                            logger.debug("Task cancelled, skip dispatch: {}", task.getTaskId());
                            return;
                        }

                        // 从映射中移除
                        taskThreads.remove(task.getTaskId());

                        // 分发执行
                        dispatchTask(task);

                    } catch (InterruptedException e) {
                        // 正常情况：任务被取消或系统关闭
                        logger.debug("Task thread interrupted: {}", task.getTaskId());
                    }
                });

        // 保存线程引用，用于取消
        taskThreads.put(task.getTaskId(), vt);

        logger.debug("Scheduled task: {}, delayMs={}", task.getTaskId(), delayMs);
    }

    /**
     * 取消调度
     * 通过 interrupt() 唤醒休眠线程
     */
    public void unschedule(String taskId) {
        Thread vt = taskThreads.remove(taskId);
        if (vt != null) {
            vt.interrupt();  // 中断休眠线程
            logger.debug("Unscheduled task: {}", taskId);
        }
    }

    /**
     * 重新调度（用于修改触发时间后）
     */
    public void reschedule(Task task) {
        unschedule(task.getTaskId());
        schedule(task);
        logger.debug("Rescheduled task: {}", task.getTaskId());
    }

    /**
     * 分发任务到执行层
     */
    private void dispatchTask(Task task) {
        String taskId = task.getTaskId();

        // 原子状态检查：只有非终态任务才能执行
        // 使用 tryStartExecution 确保状态转换的原子性
        if (!task.tryStartExecution()) {
            logger.info("Task {} skipped due to status: {} (already cancelled or terminated)",
                    taskId, task.getStatus());
            return;
        }

        // 记录唤醒延迟 (sleep 结束 → 进入分发)
        // 这是系统内部的调度精度，与 webhook 无关
        long wakeLatencyMs = System.currentTimeMillis() - task.getTriggerTime();
        MetricsCollector.getInstance().recordWakeLatency(wakeLatencyMs);
        // 同时记录旧的 trigger_latency 指标以保持兼容
        MetricsCollector.getInstance().recordTriggerLatency(wakeLatencyMs);

        // 记录唤醒时间，用于后续计算总延迟
        task.setWakeTime(System.currentTimeMillis());

        // 状态已由 tryStartExecution 设置为 DISPATCHING
        // 写入存储并记录 WAL
        taskStore.updateStatus(task, TaskStatus.SCHEDULED);

        // 写入 DISPATCH 事件
        try {
            TaskEvent dispatchEvent = TaskEvent.builder()
                    .eventId(IdGenerator.generateEventId())
                    .taskId(taskId)
                    .eventType(EventType.DISPATCH)
                    .version(task.getVersion())
                    .prevVersion(task.getVersion())
                    .eventTime(System.currentTimeMillis())
                    .build();
            walEngine.appendEvent(dispatchEvent);
        } catch (IOException e) {
            logger.error("Failed to write DISPATCH event for task: {}", taskId, e);
        }

        // 在虚拟线程中执行
        Thread.startVirtualThread(() -> dispatcher.dispatch(task));
    }

    /**
     * 获取待调度任务数量
     */
    public int getPendingCount() {
        return taskThreads.size();
    }

    /**
     * 获取统计信息
     */
    public SchedulerStats getStats() {
        return new SchedulerStats(
                taskThreads.size(),
                running.get()
        );
    }

    public record SchedulerStats(int pendingTasks, boolean running) {}
}
