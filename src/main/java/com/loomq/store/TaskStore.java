package com.loomq.store;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 任务存储
 * 内存索引，支持多种查询
 */
public class TaskStore {
    private static final Logger logger = LoggerFactory.getLogger(TaskStore.class);

    // 主索引：taskId -> Task
    private final ConcurrentHashMap<String, Task> taskById;

    // 幂等索引：idempotencyKey -> taskId
    private final ConcurrentHashMap<String, String> taskIdByIdempotencyKey;

    // 业务键索引：bizKey -> taskId
    private final ConcurrentHashMap<String, String> taskIdByBizKey;

    // 时间索引：使用跳表支持按触发时间查询
    private final SkipListIndex timeIndex;

    // 状态索引：status -> Set<taskId>
    private final Map<TaskStatus, Set<String>> taskIdsByStatus;

    // 统计
    private final AtomicLong totalTasks = new AtomicLong(0);
    private final AtomicLong version = new AtomicLong(0);

    public TaskStore() {
        this.taskById = new ConcurrentHashMap<>();
        this.taskIdByIdempotencyKey = new ConcurrentHashMap<>();
        this.taskIdByBizKey = new ConcurrentHashMap<>();
        this.timeIndex = new SkipListIndex();
        this.taskIdsByStatus = new ConcurrentHashMap<>();

        // 初始化状态索引
        for (TaskStatus status : TaskStatus.values()) {
            taskIdsByStatus.put(status, ConcurrentHashMap.newKeySet());
        }
    }

    // ========== 写操作 ==========

    /**
     * 添加任务
     */
    public void add(Task task) {
        String taskId = task.getTaskId();

        // 添加到主索引
        Task existing = taskById.putIfAbsent(taskId, task);
        if (existing != null) {
            throw new IllegalStateException("Task already exists: " + taskId);
        }

        // 添加幂等索引
        if (task.getIdempotencyKey() != null) {
            taskIdByIdempotencyKey.put(task.getIdempotencyKey(), taskId);
        }

        // 添加业务键索引
        if (task.getBizKey() != null) {
            taskIdByBizKey.put(task.getBizKey(), taskId);
        }

        // 添加时间索引
        timeIndex.add(task.getTriggerTime(), taskId);

        // 添加状态索引
        taskIdsByStatus.get(task.getStatus()).add(taskId);

        // 更新统计
        totalTasks.incrementAndGet();
        version.incrementAndGet();

        logger.debug("Added task: {}", taskId);
    }

    /**
     * 更新任务
     */
    public void update(Task task) {
        String taskId = task.getTaskId();

        Task existing = taskById.get(taskId);
        if (existing == null) {
            throw new NoSuchElementException("Task not found: " + taskId);
        }

        // 先保存旧值（因为 existing 和 task 可能是同一个对象）
        TaskStatus oldStatus = existing.getStatus();
        long oldTriggerTime = existing.getTriggerTime();
        TaskStatus newStatus = task.getStatus();
        long newTriggerTime = task.getTriggerTime();

        // 更新主索引
        taskById.put(taskId, task);

        // 更新时间索引（如果触发时间变化）
        if (oldTriggerTime != newTriggerTime) {
            timeIndex.remove(oldTriggerTime, taskId);
            timeIndex.add(newTriggerTime, taskId);
        }

        // 更新状态索引
        if (oldStatus != newStatus) {
            taskIdsByStatus.get(oldStatus).remove(taskId);
            taskIdsByStatus.get(newStatus).add(taskId);
        }

        version.incrementAndGet();

        logger.debug("Updated task: {}, status: {} -> {}", taskId, oldStatus, newStatus);
    }

    /**
     * 更新任务状态（明确指定旧状态）
     */
    public void updateStatus(Task task, TaskStatus oldStatus) {
        String taskId = task.getTaskId();
        TaskStatus newStatus = task.getStatus();

        if (oldStatus != newStatus) {
            taskIdsByStatus.get(oldStatus).remove(taskId);
            taskIdsByStatus.get(newStatus).add(taskId);
            version.incrementAndGet();
            logger.debug("Updated task status: {}, {} -> {}", taskId, oldStatus, newStatus);
        }
    }

    /**
     * 移除任务
     */
    public Task remove(String taskId) {
        Task task = taskById.remove(taskId);
        if (task == null) {
            return null;
        }

        // 移除幂等索引
        if (task.getIdempotencyKey() != null) {
            taskIdByIdempotencyKey.remove(task.getIdempotencyKey());
        }

        // 移除业务键索引
        if (task.getBizKey() != null) {
            taskIdByBizKey.remove(task.getBizKey());
        }

        // 移除时间索引
        timeIndex.remove(task.getTriggerTime(), taskId);

        // 移除状态索引
        taskIdsByStatus.get(task.getStatus()).remove(taskId);

        // 更新统计
        totalTasks.decrementAndGet();
        version.incrementAndGet();

        logger.debug("Removed task: {}", taskId);
        return task;
    }

    // ========== 读操作 ==========

    /**
     * 根据 taskId 获取任务
     */
    public Task get(String taskId) {
        return taskById.get(taskId);
    }

    /**
     * 根据幂等键获取任务
     */
    public Task getByIdempotencyKey(String idempotencyKey) {
        String taskId = taskIdByIdempotencyKey.get(idempotencyKey);
        return taskId != null ? taskById.get(taskId) : null;
    }

    /**
     * 根据业务键获取任务
     */
    public Task getByBizKey(String bizKey) {
        String taskId = taskIdByBizKey.get(bizKey);
        return taskId != null ? taskById.get(taskId) : null;
    }

    /**
     * 检查幂等键是否存在
     */
    public boolean existsByIdempotencyKey(String idempotencyKey) {
        return taskIdByIdempotencyKey.containsKey(idempotencyKey);
    }

    /**
     * 检查业务键是否存在
     */
    public boolean existsByBizKey(String bizKey) {
        return taskIdByBizKey.containsKey(bizKey);
    }

    /**
     * 获取到期任务
     */
    public List<Task> getDueTasks(long now) {
        List<String> taskIds = timeIndex.queryRange(0, now);
        return taskIds.stream()
                .map(taskById::get)
                .filter(Objects::nonNull)
                .filter(t -> t.getStatus() == TaskStatus.SCHEDULED || t.getStatus() == TaskStatus.RETRY_WAIT)
                .collect(Collectors.toList());
    }

    /**
     * 获取指定状态的任务数量
     */
    public int countByStatus(TaskStatus status) {
        return taskIdsByStatus.get(status).size();
    }

    /**
     * 获取指定状态的任务ID集合
     */
    public Set<String> getTaskIdsByStatus(TaskStatus status) {
        return Collections.unmodifiableSet(taskIdsByStatus.get(status));
    }

    // ========== 统计 ==========

    /**
     * 获取总任务数
     */
    public long size() {
        return totalTasks.get();
    }

    /**
     * 获取当前版本
     */
    public long getVersion() {
        return version.get();
    }

    /**
     * 获取统计信息
     */
    public Map<String, Long> getStats() {
        Map<String, Long> stats = new HashMap<>();
        stats.put("total", totalTasks.get());

        for (TaskStatus status : TaskStatus.values()) {
            stats.put(status.name().toLowerCase(), (long) countByStatus(status));
        }

        return stats;
    }

    /**
     * 清空所有数据
     */
    public void clear() {
        taskById.clear();
        taskIdByIdempotencyKey.clear();
        taskIdByBizKey.clear();
        timeIndex.clear();
        for (TaskStatus status : TaskStatus.values()) {
            taskIdsByStatus.get(status).clear();
        }
        totalTasks.set(0);
        version.incrementAndGet();

        logger.info("Task store cleared");
    }

    /**
     * 跳表索引（简化实现，用于按时间查询）
     */
    private static class SkipListIndex {
        private final TreeMap<Long, Set<String>> index = new TreeMap<>();

        public synchronized void add(long triggerTime, String taskId) {
            index.computeIfAbsent(triggerTime, k -> ConcurrentHashMap.newKeySet()).add(taskId);
        }

        public synchronized void remove(long triggerTime, String taskId) {
            Set<String> tasks = index.get(triggerTime);
            if (tasks != null) {
                tasks.remove(taskId);
                if (tasks.isEmpty()) {
                    index.remove(triggerTime);
                }
            }
        }

        public synchronized List<String> queryRange(long from, long to) {
            List<String> result = new ArrayList<>();
            NavigableMap<Long, Set<String>> subMap = index.subMap(from, true, to, true);
            for (Set<String> tasks : subMap.values()) {
                result.addAll(tasks);
            }
            return result;
        }

        public synchronized void clear() {
            index.clear();
        }
    }
}
