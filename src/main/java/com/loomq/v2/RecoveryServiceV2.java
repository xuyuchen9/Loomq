package com.loomq.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loomq.entity.EventType;
import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import com.loomq.wal.WalReader;
import com.loomq.wal.WalRecord;
import com.loomq.wal.v2.SyncWalWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * V0.2 恢复服务
 *
 * 从 WAL 恢复任务状态，重建调度器状态
 *
 * 恢复流程：
 * 1. 读取所有 WAL 文件
 * 2. 按 taskId 分组，取最终状态
 * 3. 过滤出需要恢复的任务（非终态）
 * 4. 重新调度未完成任务
 *
 * RTO 组成：
 * - WAL 重放时间
 * - 状态重建时间
 * - 调度恢复时间
 */
public class RecoveryServiceV2 {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryServiceV2.class);

    private final LoomqConfigV2 config;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RecoveryServiceV2(LoomqConfigV2 config) {
        this.config = config;
    }

    /**
     * 执行恢复
     *
     * @param engine 目标引擎
     * @return 恢复结果
     */
    public RecoveryResult recover(LoomqEngineV2 engine) throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("Starting V0.2 recovery from WAL: {}", config.walDataDir());

        Path walDir = Path.of(config.walDataDir());
        File[] walFiles = walDir.toFile().listFiles((dir, name) ->
                name.endsWith(".wal"));

        if (walFiles == null || walFiles.length == 0) {
            logger.info("No WAL files found, skip recovery");
            return RecoveryResult.empty();
        }

        // 任务状态收集
        Map<String, TaskState> taskStates = new HashMap<>();
        int totalRecords = 0;
        int corruptRecords = 0;

        // 读取所有 WAL 文件
        for (File walFile : walFiles) {
            logger.info("Reading WAL file: {}", walFile.getName());

            try {
                // 使用 SyncWalWriter 的读取功能
                SyncWalWriter writer = new SyncWalWriter(config.toWalConfig());
                List<WalRecord> records = readAllRecords(walFile);

                for (WalRecord record : records) {
                    totalRecords++;

                    try {
                        String taskId = record.getTaskId();
                        EventType eventType = record.getEventType();

                        TaskState state = taskStates.computeIfAbsent(taskId,
                                k -> new TaskState(taskId));

                        // 根据事件类型更新状态
                        switch (eventType) {
                            case CREATE -> handleCreate(state, record);
                            case CANCEL -> state.status = TaskStatus.CANCELLED;
                            case ACK -> state.status = TaskStatus.ACKED;
                            case RETRY -> handleRetry(state, record);
                            case FAIL -> state.status = TaskStatus.FAILED_TERMINAL;
                            default -> logger.debug("Ignore event type: {}", eventType);
                        }

                        state.lastEventTime = record.getEventTime();
                        state.version++;

                    } catch (Exception e) {
                        corruptRecords++;
                        logger.warn("Failed to process record: {}", e.getMessage());
                    }
                }

                writer.close();

            } catch (Exception e) {
                logger.error("Failed to read WAL file: {}", walFile.getName(), e);
            }
        }

        // 重建任务并恢复调度
        int tasksRecovered = 0;
        int tasksCompleted = 0;
        int tasksExpired = 0;
        long now = System.currentTimeMillis();

        for (TaskState state : taskStates.values()) {
            if (state.task == null) {
                continue;
            }

            Task task = state.task;
            if (state.status != null) {
                task.setStatus(state.status);
            }
            task.setVersion(state.version);

            // 终态任务不需要恢复调度
            if (task.getStatus().isTerminal()) {
                tasksCompleted++;
                continue;
            }

            // 检查是否过期
            if (task.getTriggerTime() < now) {
                task.setStatus(TaskStatus.EXPIRED);
                tasksExpired++;
                continue;
            }

            // 恢复调度
            boolean scheduled = engine.createTask(task).ok();
            if (scheduled) {
                tasksRecovered++;
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        logger.info("Recovery completed:");
        logger.info("  - Total records: {}", totalRecords);
        logger.info("  - Corrupt records: {}", corruptRecords);
        logger.info("  - Tasks recovered: {}", tasksRecovered);
        logger.info("  - Tasks completed: {}", tasksCompleted);
        logger.info("  - Tasks expired: {}", tasksExpired);
        logger.info("  - Duration: {} ms", duration);

        return new RecoveryResult(
                totalRecords,
                tasksRecovered,
                tasksCompleted,
                tasksExpired,
                duration
        );
    }

    private List<WalRecord> readAllRecords(File walFile) {
        // 简化实现，实际需要完整的 WAL 读取逻辑
        return List.of();
    }

    private void handleCreate(TaskState state, WalRecord record) {
        try {
            if (record.getPayload() != null && record.getPayload().length > 0) {
                String payload = new String(record.getPayload());
                state.task = objectMapper.readValue(payload, Task.class);
            }
        } catch (Exception e) {
            logger.error("Failed to parse CREATE payload", e);
        }
    }

    private void handleRetry(TaskState state, WalRecord record) {
        try {
            if (record.getPayload() != null && record.getPayload().length > 0) {
                String payload = new String(record.getPayload());
                @SuppressWarnings("unchecked")
                Map<String, Object> retryInfo = objectMapper.readValue(payload, Map.class);
                if (retryInfo.containsKey("retry_count") && state.task != null) {
                    state.task.setRetryCount(((Number) retryInfo.get("retry_count")).intValue());
                }
            }
        } catch (Exception e) {
            logger.error("Failed to parse RETRY payload", e);
        }
    }

    /**
     * 任务状态（恢复过程中使用）
     */
    private static class TaskState {
        final String taskId;
        Task task;
        TaskStatus status;
        long lastEventTime;
        int version;

        TaskState(String taskId) {
            this.taskId = taskId;
            this.version = 0;
        }
    }

    /**
     * 恢复结果
     */
    public record RecoveryResult(
            int totalRecords,
            int tasksRecovered,
            int tasksCompleted,
            int tasksExpired,
            long durationMs
    ) {
        public static RecoveryResult empty() {
            return new RecoveryResult(0, 0, 0, 0, 0);
        }
    }
}
