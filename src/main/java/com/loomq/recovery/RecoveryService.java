package com.loomq.recovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loomq.common.MetricsCollector;
import com.loomq.config.RecoveryConfig;
import com.loomq.entity.EventType;
import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalEngine;
import com.loomq.wal.WalReader;
import com.loomq.wal.WalRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 恢复服务
 * 启动时读取 WAL，重建任务状态，恢复未完成任务
 */
public class RecoveryService {
    private static final Logger logger = LoggerFactory.getLogger(RecoveryService.class);

    private final WalEngine walEngine;
    private final TaskStore taskStore;
    private final RecoveryConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RecoveryService(WalEngine walEngine, TaskStore taskStore, RecoveryConfig config) {
        this.walEngine = walEngine;
        this.taskStore = taskStore;
        this.config = config;
    }

    /**
     * 执行恢复
     */
    public void recover() throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("Starting recovery from WAL...");

        // 创建 WAL 读取器
        WalReader reader = walEngine.createReader(config.safeMode());

        // 恢复统计
        int totalRecords = 0;
        int tasksRecovered = 0;
        int tasksCompleted = 0;
        int tasksExpired = 0;

        // 任务状态收集
        Map<String, TaskBuilder> taskBuilders = new HashMap<>();

        // 限流
        Semaphore semaphore = new Semaphore(config.concurrencyLimit());
        int batchSize = config.batchSize();
        int batchCount = 0;

        try {
            // 读取所有 WAL 记录
            for (WalRecord record : reader) {
                totalRecords++;

                String taskId = record.getTaskId();
                EventType eventType = record.getEventType();

                // 获取或创建 TaskBuilder
                TaskBuilder builder = taskBuilders.computeIfAbsent(taskId, TaskBuilder::new);

                // 根据事件类型处理
                switch (eventType) {
                    case CREATE:
                        handleCreate(builder, record);
                        break;
                    case SCHEDULE:
                        builder.setStatus(TaskStatus.SCHEDULED);
                        break;
                    case DISPATCH:
                        builder.setStatus(TaskStatus.DISPATCHING);
                        break;
                    case ACK:
                        builder.setStatus(TaskStatus.ACKED);
                        break;
                    case RETRY:
                        handleRetry(builder, record);
                        break;
                    case CANCEL:
                        builder.setStatus(TaskStatus.CANCELLED);
                        break;
                    case MODIFY:
                        handleModify(builder, record);
                        break;
                    case FAIL:
                        builder.setStatus(TaskStatus.FAILED_TERMINAL);
                        break;
                    case EXPIRE:
                        builder.setStatus(TaskStatus.EXPIRED);
                        break;
                    case FIRE_NOW:
                        builder.setFired(true);
                        break;
                    default:
                        logger.warn("Unknown event type: {}", eventType);
                }

                builder.setLastEventTime(record.getEventTime());
                builder.incrementVersion();

                // 批量处理
                if (totalRecords % batchSize == 0) {
                    batchCount++;
                    if (config.sleepMs() > 0) {
                        TimeUnit.MILLISECONDS.sleep(config.sleepMs());
                    }
                    logger.debug("Recovery progress: {} records processed", totalRecords);
                }
            }

            // 构建并存储任务
            long now = System.currentTimeMillis();
            for (TaskBuilder builder : taskBuilders.values()) {
                Task task = builder.build();
                if (task == null) {
                    continue;
                }

                TaskStatus status = task.getStatus();

                // 终态任务不需要恢复到调度器
                if (status.isTerminal()) {
                    tasksCompleted++;
                    // 只存储终态任务用于查询
                    taskStore.add(task);
                    continue;
                }

                // 检查是否过期
                if (task.getTriggerTime() < now && status != TaskStatus.DISPATCHING) {
                    task.setStatus(TaskStatus.EXPIRED);
                    tasksExpired++;
                }

                // 存储任务
                taskStore.add(task);
                tasksRecovered++;

                logger.debug("Recovered task: {}, status: {}", task.getTaskId(), task.getStatus());
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Recovery interrupted", e);
        } finally {
            reader.close();
        }

        long duration = System.currentTimeMillis() - startTime;

        // 记录恢复指标
        MetricsCollector.getInstance().recordRecovery(duration, tasksRecovered);

        logger.info("Recovery completed: totalRecords={}, tasksRecovered={}, tasksCompleted={}, tasksExpired={}, duration={}ms",
                totalRecords, tasksRecovered, tasksCompleted, tasksExpired, duration);
    }

    private void handleCreate(TaskBuilder builder, WalRecord record) {
        try {
            String payload = new String(record.getPayload());
            Task task = objectMapper.readValue(payload, Task.class);
            builder.setTask(task);
        } catch (Exception e) {
            logger.error("Failed to parse CREATE event payload", e);
        }
    }

    private void handleRetry(TaskBuilder builder, WalRecord record) {
        try {
            String payload = new String(record.getPayload());
            @SuppressWarnings("unchecked")
            Map<String, Object> retryInfo = objectMapper.readValue(payload, Map.class);
            if (retryInfo.containsKey("retry_count")) {
                builder.setRetryCount(((Number) retryInfo.get("retry_count")).intValue());
            }
            builder.setStatus(TaskStatus.RETRY_WAIT);
        } catch (Exception e) {
            logger.error("Failed to parse RETRY event payload", e);
        }
    }

    private void handleModify(TaskBuilder builder, WalRecord record) {
        try {
            String payload = new String(record.getPayload());
            @SuppressWarnings("unchecked")
            Map<String, Object> modifyInfo = objectMapper.readValue(payload, Map.class);
            if (modifyInfo.containsKey("triggerTime")) {
                builder.setTriggerTime(((Number) modifyInfo.get("triggerTime")).longValue());
            }
            if (modifyInfo.containsKey("webhookUrl")) {
                builder.setWebhookUrl((String) modifyInfo.get("webhookUrl"));
            }
        } catch (Exception e) {
            logger.error("Failed to parse MODIFY event payload", e);
        }
    }

    /**
     * 任务构建器（用于恢复过程中累积状态）
     */
    private static class TaskBuilder {
        private final String taskId;
        private Task task;
        private TaskStatus status;
        private int retryCount;
        private long lastEventTime;
        private long version;
        private boolean fired;

        public TaskBuilder(String taskId) {
            this.taskId = taskId;
            this.version = 0;
        }

        public void setTask(Task task) {
            this.task = task;
        }

        public void setStatus(TaskStatus status) {
            this.status = status;
        }

        public void setRetryCount(int retryCount) {
            this.retryCount = retryCount;
        }

        public void setTriggerTime(long triggerTime) {
            if (task != null) {
                task.setTriggerTime(triggerTime);
            }
        }

        public void setWebhookUrl(String webhookUrl) {
            if (task != null) {
                task.setWebhookUrl(webhookUrl);
            }
        }

        public void setLastEventTime(long lastEventTime) {
            this.lastEventTime = lastEventTime;
        }

        public void incrementVersion() {
            this.version++;
        }

        public void setFired(boolean fired) {
            this.fired = fired;
        }

        public Task build() {
            if (task == null) {
                return null;
            }

            if (status != null) {
                task.setStatus(status);
            }
            task.setRetryCount(retryCount);
            task.setVersion(version);
            task.setUpdateTime(lastEventTime);

            return task;
        }
    }
}
