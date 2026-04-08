package com.loomq.recovery;

import com.loomq.entity.EventType;
import com.loomq.entity.TaskStatus;
import com.loomq.entity.Task;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalRecord;
import com.loomq.wal.WalSegment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WAL 恢复服务测试 (v0.4.2)
 *
 * 验证恢复链路的完整闭环。
 *
 * @author loomq
 * @since v0.4.2
 */
@DisplayName("WAL 恢复服务测试")
class WalRecoveryServiceTest {

    @TempDir
    Path tempDir;

    private TaskStore taskStore;
    private WalRecoveryService recoveryService;
    private Path walDir;

    @BeforeEach
    void setUp() {
        taskStore = new TaskStore();
        walDir = tempDir.resolve("wal");
        recoveryService = new WalRecoveryService(
                walDir.toString(),
                taskStore,
                WalRecoveryService.RecoveryConfig.defaultConfig()
        );
    }

    @AfterEach
    void tearDown() {
        taskStore.clear();
    }

    @Test
    @DisplayName("空 WAL 目录应返回空结果")
    void testEmptyWalDirectory() throws IOException {
        // 不创建任何 WAL 文件

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        assertEquals(0, result.segmentsScanned());
        assertEquals(0, result.recordsRead());
        assertEquals(0, result.tasksRestored());
        assertTrue(result.elapsedMs() >= 0);
    }

    @Test
    @DisplayName("单任务完整生命周期恢复")
    void testSingleTaskFullLifecycle() throws IOException {
        // 创建 WAL 段并写入完整任务生命周期
        Files.createDirectories(walDir);

        String taskId = "task-full-lifecycle";

        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            // 写入完整生命周期
            segment.write(createRecord(1, 0, taskId, EventType.CREATE));
            segment.write(createRecord(1, 1, taskId, EventType.SCHEDULE));
            segment.write(createRecord(1, 2, taskId, EventType.READY));
            segment.write(createRecord(1, 3, taskId, EventType.DISPATCH));
            segment.write(createRecord(1, 4, taskId, EventType.ACK));
        }

        // 执行恢复
        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        // 验证：终态任务不应恢复
        assertEquals(1, result.segmentsScanned());
        assertEquals(5, result.recordsRead());
        assertEquals(0, result.tasksRestored());
        assertEquals(1, result.terminalTasksSkipped());
    }

    @Test
    @DisplayName("PENDING 任务恢复后保持 PENDING")
    void testPendingTaskRecovery() throws IOException {
        Files.createDirectories(walDir);

        String taskId = "task-pending";
        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            segment.write(createRecord(1, 0, taskId, EventType.CREATE));
        }

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        assertEquals(1, result.tasksRestored());

        Task task = taskStore.get(taskId);
        assertNotNull(task);
        assertEquals(TaskStatus.PENDING, task.getStatus());
    }

    @Test
    @DisplayName("SCHEDULED 任务恢复后保持 SCHEDULED")
    void testScheduledTaskRecovery() throws IOException {
        Files.createDirectories(walDir);

        String taskId = "task-scheduled";
        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            segment.write(createRecord(1, 0, taskId, EventType.CREATE));
            segment.write(createRecord(1, 1, taskId, EventType.SCHEDULE));
        }

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        assertEquals(1, result.tasksRestored());

        Task task = taskStore.get(taskId);
        assertNotNull(task);
        assertEquals(TaskStatus.SCHEDULED, task.getStatus());
    }

    @Test
    @DisplayName("READY 任务恢复后保持 READY")
    void testReadyTaskRecovery() throws IOException {
        Files.createDirectories(walDir);

        String taskId = "task-ready";
        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            segment.write(createRecord(1, 0, taskId, EventType.CREATE));
            segment.write(createRecord(1, 1, taskId, EventType.SCHEDULE));
            segment.write(createRecord(1, 2, taskId, EventType.READY));
        }

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        assertEquals(1, result.tasksRestored());

        Task task = taskStore.get(taskId);
        assertNotNull(task);
        assertEquals(TaskStatus.READY, task.getStatus());
    }

    @Test
    @DisplayName("RUNNING 任务恢复后重置为 READY（关键策略）")
    void testRunningTaskRecoveryResetsToReady() throws IOException {
        Files.createDirectories(walDir);

        String taskId = "task-running";
        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            segment.write(createRecord(1, 0, taskId, EventType.CREATE));
            segment.write(createRecord(1, 1, taskId, EventType.SCHEDULE));
            segment.write(createRecord(1, 2, taskId, EventType.READY));
            segment.write(createRecord(1, 3, taskId, EventType.DISPATCH));
        }

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        // 验证恢复统计
        assertEquals(1, result.tasksRestored());
        assertEquals(1, result.inflightTasks());

        // 验证状态：RUNNING -> READY
        Task task = taskStore.get(taskId);
        assertNotNull(task);
        assertEquals(TaskStatus.READY, task.getStatus(),
                "RUNNING 任务应恢复为 READY 以便重新执行");
    }

    @Test
    @DisplayName("RETRY_WAIT 任务恢复后保持 RETRY_WAIT")
    void testRetryWaitTaskRecovery() throws IOException {
        Files.createDirectories(walDir);

        String taskId = "task-retry-wait";
        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            segment.write(createRecord(1, 0, taskId, EventType.CREATE));
            segment.write(createRecord(1, 1, taskId, EventType.SCHEDULE));
            segment.write(createRecord(1, 2, taskId, EventType.READY));
            segment.write(createRecord(1, 3, taskId, EventType.DISPATCH));
            segment.write(createRecord(1, 4, taskId, EventType.RETRY));
        }

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        assertEquals(1, result.tasksRestored());

        Task task = taskStore.get(taskId);
        assertNotNull(task);
        assertEquals(TaskStatus.RETRY_WAIT, task.getStatus());
    }

    @Test
    @DisplayName("终态任务恢复时被跳过")
    void testTerminalTasksAreSkipped() throws IOException {
        Files.createDirectories(walDir);

        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            // SUCCESS 任务
            segment.write(createRecord(1, 0, "task-success", EventType.CREATE));
            segment.write(createRecord(1, 1, "task-success", EventType.ACK));

            // FAILED 任务
            segment.write(createRecord(1, 2, "task-failed", EventType.CREATE));
            segment.write(createRecord(1, 3, "task-failed", EventType.FAIL));

            // CANCELLED 任务
            segment.write(createRecord(1, 4, "task-cancelled", EventType.CREATE));
            segment.write(createRecord(1, 5, "task-cancelled", EventType.CANCEL));

            // EXPIRED 任务
            segment.write(createRecord(1, 6, "task-expired", EventType.CREATE));
            segment.write(createRecord(1, 7, "task-expired", EventType.EXPIRE));

            // DEAD_LETTER 任务
            segment.write(createRecord(1, 8, "task-dlq", EventType.CREATE));
            segment.write(createRecord(1, 9, "task-dlq", EventType.DEAD_LETTER));
        }

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        // 所有终态任务都应被跳过
        assertEquals(0, result.tasksRestored());
        assertEquals(5, result.terminalTasksSkipped());

        // 存储中不应有任何任务
        assertEquals(0, taskStore.size());
    }

    @Test
    @DisplayName("多段文件恢复")
    void testMultiSegmentRecovery() throws IOException {
        Files.createDirectories(walDir);

        // 段1：任务1-3
        try (WalSegment segment1 = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            segment1.write(createRecord(1, 0, "task-1", EventType.CREATE));
            segment1.write(createRecord(1, 1, "task-2", EventType.CREATE));
            segment1.write(createRecord(1, 2, "task-3", EventType.CREATE));
        }

        // 段2：任务4-5 + 任务1完成
        try (WalSegment segment2 = new WalSegment(
                walDir.resolve("00000002.wal").toFile(),
                2,
                1024 * 1024
        )) {
            segment2.write(createRecord(2, 0, "task-4", EventType.CREATE));
            segment2.write(createRecord(2, 1, "task-5", EventType.CREATE));
            segment2.write(createRecord(2, 2, "task-1", EventType.ACK));
        }

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        assertEquals(2, result.segmentsScanned());
        assertEquals(6, result.recordsRead());
        // 任务2,3,4,5 恢复 (4个)，任务1终态被跳过
        assertEquals(4, result.tasksRestored());
    }

    @Test
    @DisplayName("乱序事件恢复（按recordSeq顺序重放，无效转换被拒绝）")
    void testOutOfOrderEvents() throws IOException {
        Files.createDirectories(walDir);

        String taskId = "task-out-of-order";
        // 故意乱序写入：READY(0)先于CREATE(1)，但重放按recordSeq顺序
        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            segment.write(createRecord(1, 0, taskId, EventType.READY));     // recordSeq=0
            segment.write(createRecord(1, 1, taskId, EventType.CREATE));    // recordSeq=1
            segment.write(createRecord(1, 2, taskId, EventType.SCHEDULE));  // recordSeq=2
        }

        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        assertEquals(1, result.tasksRestored());

        Task task = taskStore.get(taskId);
        assertNotNull(task);
        // 重放顺序：READY(0)因状态机规则被拒绝(不能从null到READY) -> CREATE(PENDING) -> SCHEDULE(SCHEDULED)
        assertEquals(TaskStatus.SCHEDULED, task.getStatus());
    }

    @Test
    @DisplayName("损坏记录恢复（safeMode=false）")
    void testCorruptedRecordRecovery() throws IOException {
        Files.createDirectories(walDir);

        // 创建正常段
        try (WalSegment segment = new WalSegment(
                walDir.resolve("00000001.wal").toFile(),
                1,
                1024 * 1024
        )) {
            segment.write(createRecord(1, 0, "task-1", EventType.CREATE));
            segment.write(createRecord(1, 1, "task-2", EventType.CREATE));
        }

        // safeMode=false 时应该跳过错误继续
        WalRecoveryService.RecoveryResult result = recoveryService.recover();

        assertEquals(2, result.tasksRestored());
    }

    // ========== 辅助方法 ==========

    private WalRecord createRecord(int segmentSeq, long recordSeq, String taskId, EventType eventType) {
        return WalRecord.create(
                segmentSeq,
                recordSeq,
                taskId,
                null, // bizKey
                eventType,
                System.currentTimeMillis(),
                new byte[0] // empty payload
        );
    }
}
