package com.loomq.recovery;

import com.loomq.entity.EventType;
import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalReader;
import com.loomq.wal.WalRecord;
import com.loomq.wal.WalSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WAL 恢复服务 (v0.4.2)
 *
 * 实现完整的恢复链路闭环：
 * 1. 扫描 WAL 目录，收集所有 segment 文件
 * 2. 按序号排序，构建 WalReader
 * 3. 顺序读取所有记录，重建任务状态机
 * 4. 应用恢复策略，重建 TaskStore
 *
 * 恢复策略：
 * | 状态        | 恢复动作                 |
 * |-------------|--------------------------|
 * | PENDING     | 放入调度器等待调度       |
 * | SCHEDULED   | 放入调度器继续等待       |
 * | READY       | 标记为 READY 待执行      |
 * | RUNNING     | 重置为 READY 重新执行    |
 * | RETRY_WAIT  | 放入调度器等待重试       |
 * | 终态        | 忽略，不恢复             |
 *
 * @author loomq
 * @since v0.4.2
 */
public class WalRecoveryService {

    private static final Logger logger = LoggerFactory.getLogger(WalRecoveryService.class);

    private final String walDir;
    private final TaskStore taskStore;
    private final RecoveryConfig config;

    // 恢复统计
    private final RecoveryStats stats = new RecoveryStats();

    public WalRecoveryService(String walDir, TaskStore taskStore, RecoveryConfig config) {
        this.walDir = walDir;
        this.taskStore = taskStore;
        this.config = config;
    }

    /**
     * 执行完整恢复流程
     *
     * @return 恢复结果
     */
    public RecoveryResult recover() throws IOException {
        long startTime = System.currentTimeMillis();
        stats.reset();

        logger.info("Starting WAL recovery from directory: {}", walDir);

        // 1. 扫描 WAL 文件
        List<WalSegment> segments = scanWalSegments();
        if (segments.isEmpty()) {
            logger.info("No WAL segments found, recovery skipped");
            return RecoveryResult.empty();
        }

        try {
            stats.segmentsScanned = segments.size();
            logger.info("Found {} WAL segments", segments.size());

            // 2. 读取所有 WAL 记录
            List<WalRecord> records = readAllRecords(segments);
            stats.recordsRead = records.size();
            logger.info("Read {} WAL records", records.size());

            // 3. 重放记录，重建任务状态
            Map<String, TaskRecoveryState> taskStates = replayRecords(records);
            stats.tasksRecovered = taskStates.size();
            logger.info("Rebuilt {} tasks from WAL", taskStates.size());

            // 4. 应用恢复策略，重建存储
            int restored = restoreTasks(taskStates);
            stats.tasksRestored = restored;

            long elapsedMs = System.currentTimeMillis() - startTime;
            stats.elapsedMs = elapsedMs;

            logger.info("Recovery completed in {}ms: {} segments, {} records, {} tasks recovered, {} tasks restored",
                    elapsedMs, stats.segmentsScanned, stats.recordsRead, stats.tasksRecovered, stats.tasksRestored);

            return new RecoveryResult(
                    stats.segmentsScanned,
                    stats.recordsRead,
                    stats.tasksRecovered,
                    stats.tasksRestored,
                    stats.inflightTasks,
                    stats.terminalTasksSkipped,
                    elapsedMs
            );
        } finally {
            // 关闭所有 segment 释放文件句柄
            for (WalSegment segment : segments) {
                try {
                    segment.close();
                } catch (Exception e) {
                    logger.warn("Failed to close WAL segment: {}", segment.getFile(), e);
                }
            }
        }
    }

    /**
     * 扫描 WAL 目录，收集所有 segment 文件
     */
    private List<WalSegment> scanWalSegments() throws IOException {
        List<WalSegment> segments = new ArrayList<>();
        Path walPath = Paths.get(walDir);

        if (!Files.exists(walPath)) {
            logger.warn("WAL directory does not exist: {}", walDir);
            return segments;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(walPath, "*.wal")) {
            for (Path path : stream) {
                try {
                    String fileName = path.getFileName().toString();
                    int segmentSeq = WalSegment.parseSegmentSeq(fileName);
                    WalSegment segment = new WalSegment(
                            path.toFile(),
                            segmentSeq,
                            1024 * 1024 * 1024, // 1GB max size
                            true // read-only
                    );
                    segments.add(segment);
                    logger.debug("Found WAL segment: {} (seq={})", fileName, segmentSeq);
                } catch (Exception e) {
                    logger.warn("Failed to parse WAL segment: {}", path, e);
                }
            }
        }

        // 按序号排序
        segments.sort(Comparator.comparingInt(WalSegment::getSegmentSeq));
        return segments;
    }

    /**
     * 读取所有 WAL 记录
     */
    private List<WalRecord> readAllRecords(List<WalSegment> segments) throws IOException {
        WalReader reader = new WalReader(segments, config.safeMode());
        return reader.readAll();
    }

    /**
     * 重放 WAL 记录，重建任务状态
     */
    private Map<String, TaskRecoveryState> replayRecords(List<WalRecord> records) {
        Map<String, TaskRecoveryState> taskStates = new LinkedHashMap<>();

        for (WalRecord record : records) {
            try {
                String taskId = record.getTaskId();
                EventType eventType = record.getEventType();

                // 获取或创建任务状态
                TaskRecoveryState state = taskStates.computeIfAbsent(taskId, TaskRecoveryState::new);

                // 应用事件
                applyEvent(state, record);
                stats.recordsReplayed++;

                // 追踪最大序列号
                stats.maxRecordSeq = Math.max(stats.maxRecordSeq, record.getRecordSeq());

            } catch (Exception e) {
                stats.recordsFailed++;
                if (config.safeMode()) {
                    logger.error("Failed to replay record seq={}, stopping recovery", record.getRecordSeq(), e);
                    break;
                } else {
                    logger.warn("Failed to replay record seq={}, skipping", record.getRecordSeq(), e);
                }
            }
        }

        return taskStates;
    }

    /**
     * 应用事件到任务状态
     */
    private void applyEvent(TaskRecoveryState state, WalRecord record) {
        EventType eventType = record.getEventType();

        switch (eventType) {
            case CREATE -> {
                // 初始化任务
                state.taskId = record.getTaskId();
                state.status = TaskStatus.PENDING;
                state.createTime = record.getEventTime();
                // TODO: 从 payload 解析完整任务数据
            }
            case SCHEDULE -> {
                if (state.status == TaskStatus.PENDING || state.status == TaskStatus.RETRY_WAIT) {
                    state.status = TaskStatus.SCHEDULED;
                    state.scheduledTime = record.getEventTime();
                }
            }
            case READY -> {
                if (state.status == TaskStatus.SCHEDULED || state.status == TaskStatus.RETRY_WAIT) {
                    state.status = TaskStatus.READY;
                    state.readyTime = record.getEventTime();
                }
            }
            case DISPATCH -> {
                if (state.status == TaskStatus.READY) {
                    state.status = TaskStatus.RUNNING;
                    state.executionStartTime = record.getEventTime();
                    state.attemptCount++;
                }
            }
            case ACK -> {
                state.status = TaskStatus.SUCCESS;
                state.completionTime = record.getEventTime();
            }
            case RETRY -> {
                if (state.status == TaskStatus.RUNNING) {
                    state.status = TaskStatus.RETRY_WAIT;
                    state.retryCount++;
                }
            }
            case FAIL -> {
                state.status = TaskStatus.FAILED;
                state.completionTime = record.getEventTime();
                // TODO: 从 payload 解析错误信息
            }
            case CANCEL -> {
                state.status = TaskStatus.CANCELLED;
                state.completionTime = record.getEventTime();
            }
            case EXPIRE -> {
                state.status = TaskStatus.EXPIRED;
                state.completionTime = record.getEventTime();
            }
            case DEAD_LETTER -> {
                state.status = TaskStatus.DEAD_LETTER;
                state.completionTime = record.getEventTime();
            }
            case MODIFY -> {
                // TODO: 应用修改
            }
            case FIRE_NOW -> {
                // 立即触发：强制设置为 READY
                state.status = TaskStatus.READY;
            }
            case CHECKPOINT -> {
                state.lastCheckpointSeq = record.getRecordSeq();
            }
        }

        state.lastEventTime = record.getEventTime();
        state.lastRecordSeq = record.getRecordSeq();
    }

    /**
     * 恢复任务到存储
     *
     * @return 成功恢复的任务数
     */
    private int restoreTasks(Map<String, TaskRecoveryState> taskStates) {
        int restored = 0;

        for (TaskRecoveryState state : taskStates.values()) {
            try {
                // 跳过终态任务
                if (isTerminalState(state.status)) {
                    stats.terminalTasksSkipped++;
                    logger.debug("Skipping terminal task {} (status={})", state.taskId, state.status);
                    continue;
                }

                // 应用恢复策略
                TaskStatus restoredStatus = applyRecoveryStrategy(state);

                // 创建任务对象
                Task task = createTaskFromState(state, restoredStatus);

                // 添加到存储
                taskStore.add(task);
                restored++;

                // 统计
                if (state.status == TaskStatus.RUNNING) {
                    stats.inflightTasks++;
                }

                logger.debug("Restored task {}: {} -> {}",
                        state.taskId, state.status, restoredStatus);

            } catch (Exception e) {
                logger.error("Failed to restore task {}", state.taskId, e);
            }
        }

        return restored;
    }

    /**
     * 应用恢复策略
     *
     * 关键策略：RUNNING 任务重置为 READY 重新执行
     * 原因：无法确定 RUNNING 任务是否已完成
     * 要求：下游服务必须幂等
     */
    private TaskStatus applyRecoveryStrategy(TaskRecoveryState state) {
        return switch (state.status) {
            case PENDING, SCHEDULED, RETRY_WAIT -> {
                // 保持原状态，调度器会继续处理
                yield state.status;
            }
            case READY -> {
                // 已经是 READY，保持
                yield TaskStatus.READY;
            }
            case RUNNING -> {
                // 关键：RUNNING -> READY
                // 无法确定是否已完成，安全做法是重新执行
                stats.runningTasksReset++;
                logger.info("Reset RUNNING task {} to READY (will re-execute)", state.taskId);
                yield TaskStatus.READY;
            }
            default -> throw new IllegalStateException("Unexpected status: " + state.status);
        };
    }

    /**
     * 从状态创建任务对象
     */
    private Task createTaskFromState(TaskRecoveryState state, TaskStatus restoredStatus) {
        Task task = Task.builder()
                .taskId(state.taskId)
                .webhookUrl("recovered") // TODO: 从 payload 解析
                .wakeTime(state.scheduledTime > 0 ? state.scheduledTime : System.currentTimeMillis())
                .build();

        // 强制设置恢复后的状态
        task.getLifecycle().forceSetStatus(restoredStatus);

        // 恢复重试计数
        task.setRetryCount(state.retryCount);

        return task;
    }

    /**
     * 判断是否为终态
     */
    private boolean isTerminalState(TaskStatus status) {
        return status == TaskStatus.SUCCESS ||
               status == TaskStatus.FAILED ||
               status == TaskStatus.CANCELLED ||
               status == TaskStatus.EXPIRED ||
               status == TaskStatus.DEAD_LETTER;
    }

    // ========== 内部类 ==========

    /**
     * 任务恢复状态（临时）
     */
    private static class TaskRecoveryState {
        String taskId;
        TaskStatus status = TaskStatus.PENDING;
        int attemptCount = 0;
        int retryCount = 0;
        long createTime = 0;
        long scheduledTime = 0;
        long readyTime = 0;
        long executionStartTime = 0;
        long completionTime = 0;
        long lastEventTime = 0;
        long lastRecordSeq = 0;
        long lastCheckpointSeq = -1;

        TaskRecoveryState(String taskId) {
            this.taskId = taskId;
        }
    }

    /**
     * 恢复统计
     */
    public static class RecoveryStats {
        int segmentsScanned = 0;
        int recordsRead = 0;
        int recordsReplayed = 0;
        int recordsFailed = 0;
        int tasksRecovered = 0;
        int tasksRestored = 0;
        int inflightTasks = 0;
        int terminalTasksSkipped = 0;
        int runningTasksReset = 0;
        long maxRecordSeq = 0;
        long elapsedMs = 0;

        void reset() {
            segmentsScanned = 0;
            recordsRead = 0;
            recordsReplayed = 0;
            recordsFailed = 0;
            tasksRecovered = 0;
            tasksRestored = 0;
            inflightTasks = 0;
            terminalTasksSkipped = 0;
            runningTasksReset = 0;
            maxRecordSeq = 0;
            elapsedMs = 0;
        }
    }

    /**
     * 恢复结果
     */
    public record RecoveryResult(
            int segmentsScanned,
            int recordsRead,
            int tasksRecovered,
            int tasksRestored,
            int inflightTasks,
            int terminalTasksSkipped,
            long elapsedMs
    ) {
        public static RecoveryResult empty() {
            return new RecoveryResult(0, 0, 0, 0, 0, 0, 0);
        }

        @Override
        public String toString() {
            return String.format(
                    "RecoveryResult{segments=%d, records=%d, tasksRecovered=%d, tasksRestored=%d, " +
                    "inflight=%d, terminalSkipped=%d, elapsed=%dms}",
                    segmentsScanned, recordsRead, tasksRecovered, tasksRestored,
                    inflightTasks, terminalTasksSkipped, elapsedMs);
        }
    }

    /**
     * 恢复配置
     */
    public record RecoveryConfig(
            boolean safeMode,      // true: 遇到错误停止; false: 跳过错误继续
            boolean resetRunning   // true: RUNNING -> READY; false: 保持 RUNNING
    ) {
        public static RecoveryConfig defaultConfig() {
            return new RecoveryConfig(false, true);
        }
    }
}
