package com.loomq.entity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TaskLifecycle 状态机单元测试
 *
 * 验证所有状态转换规则，确保状态机语义正确。
 *
 * @author loomq
 * @since v0.4.1
 */
@DisplayName("TaskLifecycle 状态机测试")
class TaskLifecycleTest {

    private static final String TEST_TASK_ID = "test-task-001";

    private TaskLifecycle lifecycle;

    @BeforeEach
    void setUp() {
        lifecycle = new TaskLifecycle(TEST_TASK_ID);
    }

    @Nested
    @DisplayName("初始状态测试")
    class InitialStateTest {

        @Test
        @DisplayName("初始状态应为 PENDING")
        void testInitialStateIsPending() {
            assertEquals(TaskStatus.PENDING, lifecycle.getStatus());
        }

        @Test
        @DisplayName("初始创建时间应已设置")
        void testInitialCreateTimeIsSet() {
            assertTrue(lifecycle.getCreateTime() > 0);
            assertTrue(lifecycle.getCreateTime() <= System.currentTimeMillis());
        }
    }

    @Nested
    @DisplayName("PENDING 状态转换测试")
    class PendingTransitionTest {

        @Test
        @DisplayName("PENDING -> SCHEDULED 应成功")
        void testPendingToScheduled() {
            assertTrue(lifecycle.transitionToScheduled());
            assertEquals(TaskStatus.SCHEDULED, lifecycle.getStatus());
            assertTrue(lifecycle.getScheduledTime() > 0);
        }

        @Test
        @DisplayName("PENDING -> CANCELLED 应成功")
        void testPendingToCancelled() {
            assertTrue(lifecycle.cancel());
            assertEquals(TaskStatus.CANCELLED, lifecycle.getStatus());
            assertTrue(lifecycle.getCompletionTime() > 0);
        }

        @Test
        @DisplayName("PENDING -> READY 应失败")
        void testPendingToReadyShouldFail() {
            assertFalse(lifecycle.transitionToReady());
            assertEquals(TaskStatus.PENDING, lifecycle.getStatus());
        }

        @Test
        @DisplayName("PENDING -> RUNNING 应失败")
        void testPendingToRunningShouldFail() {
            assertFalse(lifecycle.transitionToRunning());
            assertEquals(TaskStatus.PENDING, lifecycle.getStatus());
        }

        @Test
        @DisplayName("PENDING -> SUCCESS 应失败")
        void testPendingToSuccessShouldFail() {
            assertFalse(lifecycle.transitionToSuccess());
            assertEquals(TaskStatus.PENDING, lifecycle.getStatus());
        }

        @Test
        @DisplayName("PENDING -> FAILED 应失败")
        void testPendingToFailedShouldFail() {
            assertFalse(lifecycle.transitionToFailed("error"));
            assertEquals(TaskStatus.PENDING, lifecycle.getStatus());
        }
    }

    @Nested
    @DisplayName("SCHEDULED 状态转换测试")
    class ScheduledTransitionTest {

        @BeforeEach
        void setUpScheduled() {
            lifecycle.transitionToScheduled();
            assertEquals(TaskStatus.SCHEDULED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("SCHEDULED -> READY 应成功")
        void testScheduledToReady() {
            assertTrue(lifecycle.transitionToReady());
            assertEquals(TaskStatus.READY, lifecycle.getStatus());
            assertTrue(lifecycle.getReadyTime() > 0);
        }

        @Test
        @DisplayName("SCHEDULED -> CANCELLED 应成功")
        void testScheduledToCancelled() {
            assertTrue(lifecycle.cancel());
            assertEquals(TaskStatus.CANCELLED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("SCHEDULED -> EXPIRED 应成功")
        void testScheduledToExpired() {
            assertTrue(lifecycle.expire());
            assertEquals(TaskStatus.EXPIRED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("SCHEDULED -> RUNNING 应失败")
        void testScheduledToRunningShouldFail() {
            assertFalse(lifecycle.transitionToRunning());
            assertEquals(TaskStatus.SCHEDULED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("SCHEDULED -> SUCCESS 应失败")
        void testScheduledToSuccessShouldFail() {
            assertFalse(lifecycle.transitionToSuccess());
            assertEquals(TaskStatus.SCHEDULED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("SCHEDULED -> RETRY_WAIT 应失败")
        void testScheduledToRetryWaitShouldFail() {
            assertFalse(lifecycle.transitionToRetryWait(3));
            assertEquals(TaskStatus.SCHEDULED, lifecycle.getStatus());
        }
    }

    @Nested
    @DisplayName("READY 状态转换测试")
    class ReadyTransitionTest {

        @BeforeEach
        void setUpReady() {
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            assertEquals(TaskStatus.READY, lifecycle.getStatus());
        }

        @Test
        @DisplayName("READY -> RUNNING 应成功")
        void testReadyToRunning() {
            assertTrue(lifecycle.transitionToRunning());
            assertEquals(TaskStatus.RUNNING, lifecycle.getStatus());
            assertTrue(lifecycle.getExecutionStartTime() > 0);
            assertEquals(1, lifecycle.getAttemptCount());
        }

        @Test
        @DisplayName("READY -> CANCELLED 应成功")
        void testReadyToCancelled() {
            assertTrue(lifecycle.cancel());
            assertEquals(TaskStatus.CANCELLED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("READY -> SUCCESS 应失败")
        void testReadyToSuccessShouldFail() {
            assertFalse(lifecycle.transitionToSuccess());
            assertEquals(TaskStatus.READY, lifecycle.getStatus());
        }

        @Test
        @DisplayName("READY -> SCHEDULED 应失败")
        void testReadyToScheduledShouldFail() {
            assertFalse(lifecycle.transitionToScheduled());
            assertEquals(TaskStatus.READY, lifecycle.getStatus());
        }
    }

    @Nested
    @DisplayName("RUNNING 状态转换测试")
    class RunningTransitionTest {

        @BeforeEach
        void setUpRunning() {
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            lifecycle.transitionToRunning();
            assertEquals(TaskStatus.RUNNING, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RUNNING -> SUCCESS 应成功")
        void testRunningToSuccess() {
            assertTrue(lifecycle.transitionToSuccess());
            assertEquals(TaskStatus.SUCCESS, lifecycle.getStatus());
            assertTrue(lifecycle.getCompletionTime() > 0);
        }

        @Test
        @DisplayName("RUNNING -> RETRY_WAIT 应成功（未达最大重试）")
        void testRunningToRetryWait() {
            assertTrue(lifecycle.transitionToRetryWait(3));
            assertEquals(TaskStatus.RETRY_WAIT, lifecycle.getStatus());
            assertEquals(1, lifecycle.getRetryCount());
        }

        @Test
        @DisplayName("RUNNING -> RETRY_WAIT 应进入 DEAD_LETTER（已达最大重试）")
        void testRunningToDeadLetterWhenMaxRetryReached() {
            // maxRetryCount=0 表示不允许重试，直接进入 DEAD_LETTER
            assertTrue(lifecycle.transitionToRetryWait(0));
            assertEquals(TaskStatus.DEAD_LETTER, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RUNNING -> RETRY_WAIT 应进入 DEAD_LETTER（重试耗尽）")
        void testRunningToDeadLetterWhenRetryExhausted() {
            // 先进行 2 次重试
            lifecycle.transitionToRetryWait(3);
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            lifecycle.transitionToRunning();
            lifecycle.transitionToRetryWait(3);
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            lifecycle.transitionToRunning();

            // 第 3 次失败应进入 DEAD_LETTER
            assertTrue(lifecycle.transitionToRetryWait(3));
            assertEquals(TaskStatus.DEAD_LETTER, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RUNNING -> FAILED 应成功")
        void testRunningToFailed() {
            assertTrue(lifecycle.transitionToFailed("execution error"));
            assertEquals(TaskStatus.FAILED, lifecycle.getStatus());
            assertEquals("execution error", lifecycle.getLastError());
            assertTrue(lifecycle.getLastErrorTime() > 0);
        }

        @Test
        @DisplayName("RUNNING -> DEAD_LETTER 应成功")
        void testRunningToDeadLetter() {
            assertTrue(lifecycle.transitionToDeadLetter("permanent failure"));
            assertEquals(TaskStatus.DEAD_LETTER, lifecycle.getStatus());
            assertEquals("permanent failure", lifecycle.getLastError());
        }

        @Test
        @DisplayName("RUNNING -> CANCELLED 应失败")
        void testRunningToCancelledShouldFail() {
            assertFalse(lifecycle.cancel());
            assertEquals(TaskStatus.RUNNING, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RUNNING -> SCHEDULED 应失败")
        void testRunningToScheduledShouldFail() {
            assertFalse(lifecycle.transitionToScheduled());
            assertEquals(TaskStatus.RUNNING, lifecycle.getStatus());
        }
    }

    @Nested
    @DisplayName("RETRY_WAIT 状态转换测试")
    class RetryWaitTransitionTest {

        @BeforeEach
        void setUpRetryWait() {
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            lifecycle.transitionToRunning();
            lifecycle.transitionToRetryWait(3);
            assertEquals(TaskStatus.RETRY_WAIT, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RETRY_WAIT -> SCHEDULED 应成功")
        void testRetryWaitToScheduled() {
            assertTrue(lifecycle.transitionToScheduled());
            assertEquals(TaskStatus.SCHEDULED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RETRY_WAIT -> FAILED 应成功")
        void testRetryWaitToFailed() {
            assertTrue(lifecycle.transitionToFailed("give up"));
            assertEquals(TaskStatus.FAILED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RETRY_WAIT -> DEAD_LETTER 应成功")
        void testRetryWaitToDeadLetter() {
            assertTrue(lifecycle.transitionToDeadLetter("manual dead letter"));
            assertEquals(TaskStatus.DEAD_LETTER, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RETRY_WAIT -> CANCELLED 应成功")
        void testRetryWaitToCancelled() {
            assertTrue(lifecycle.cancel());
            assertEquals(TaskStatus.CANCELLED, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RETRY_WAIT -> RUNNING 应失败")
        void testRetryWaitToRunningShouldFail() {
            assertFalse(lifecycle.transitionToRunning());
            assertEquals(TaskStatus.RETRY_WAIT, lifecycle.getStatus());
        }

        @Test
        @DisplayName("RETRY_WAIT -> SUCCESS 应失败")
        void testRetryWaitToSuccessShouldFail() {
            assertFalse(lifecycle.transitionToSuccess());
            assertEquals(TaskStatus.RETRY_WAIT, lifecycle.getStatus());
        }
    }

    @Nested
    @DisplayName("终态不可转换测试")
    class TerminalStateTest {

        @ParameterizedTest
        @EnumSource(value = TaskStatus.class, names = {"SUCCESS", "FAILED", "CANCELLED", "EXPIRED", "DEAD_LETTER"})
        @DisplayName("终态任务应拒绝所有转换")
        void testTerminalStateCannotTransition(TaskStatus terminalStatus) {
            // 强制设置到终态
            lifecycle.forceSetStatus(terminalStatus);
            assertEquals(terminalStatus, lifecycle.getStatus());
            assertTrue(lifecycle.getStatus().isTerminal());

            // 所有转换都应失败
            assertFalse(lifecycle.transitionToScheduled());
            assertFalse(lifecycle.transitionToReady());
            assertFalse(lifecycle.transitionToRunning());
            assertFalse(lifecycle.transitionToSuccess());
            assertFalse(lifecycle.transitionToRetryWait(3));
            assertFalse(lifecycle.transitionToFailed("error"));
            assertFalse(lifecycle.transitionToDeadLetter("error"));
            assertFalse(lifecycle.cancel());
            assertFalse(lifecycle.expire());

            // 状态应保持不变
            assertEquals(terminalStatus, lifecycle.getStatus());
        }
    }

    @Nested
    @DisplayName("状态验证工具方法测试")
    class ValidationMethodTest {

        @Test
        @DisplayName("isValidTransition 应正确判断合法流转")
        void testIsValidTransition() {
            // 合法流转
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.PENDING, TaskStatus.SCHEDULED));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.PENDING, TaskStatus.CANCELLED));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.SCHEDULED, TaskStatus.READY));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.SCHEDULED, TaskStatus.CANCELLED));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.SCHEDULED, TaskStatus.EXPIRED));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.READY, TaskStatus.RUNNING));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.READY, TaskStatus.CANCELLED));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.RUNNING, TaskStatus.SUCCESS));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.RUNNING, TaskStatus.RETRY_WAIT));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.RUNNING, TaskStatus.FAILED));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.RUNNING, TaskStatus.DEAD_LETTER));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.RETRY_WAIT, TaskStatus.SCHEDULED));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.RETRY_WAIT, TaskStatus.FAILED));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.RETRY_WAIT, TaskStatus.DEAD_LETTER));
            assertTrue(TaskLifecycle.isValidTransition(TaskStatus.RETRY_WAIT, TaskStatus.CANCELLED));
        }

        @Test
        @DisplayName("isValidTransition 应正确判断非法流转")
        void testIsInvalidTransition() {
            // 相同状态
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.PENDING, TaskStatus.PENDING));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.RUNNING, TaskStatus.RUNNING));

            // 终态不能转换
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.SUCCESS, TaskStatus.PENDING));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.FAILED, TaskStatus.SCHEDULED));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.CANCELLED, TaskStatus.READY));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.EXPIRED, TaskStatus.RUNNING));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.DEAD_LETTER, TaskStatus.SUCCESS));

            // 非法路径
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.PENDING, TaskStatus.READY));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.PENDING, TaskStatus.RUNNING));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.SCHEDULED, TaskStatus.RUNNING));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.READY, TaskStatus.SUCCESS));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.RUNNING, TaskStatus.CANCELLED));
            assertFalse(TaskLifecycle.isValidTransition(TaskStatus.RETRY_WAIT, TaskStatus.SUCCESS));
        }

        @Test
        @DisplayName("canTransitionTo 应正确判断")
        void testCanTransitionTo() {
            assertTrue(lifecycle.canTransitionTo(TaskStatus.SCHEDULED));
            assertTrue(lifecycle.canTransitionTo(TaskStatus.CANCELLED));
            assertFalse(lifecycle.canTransitionTo(TaskStatus.READY));
            assertFalse(lifecycle.canTransitionTo(TaskStatus.RUNNING));

            lifecycle.transitionToScheduled();
            assertTrue(lifecycle.canTransitionTo(TaskStatus.READY));
            assertFalse(lifecycle.canTransitionTo(TaskStatus.SCHEDULED)); // 已经是该状态
        }
    }

    @Nested
    @DisplayName("执行计数测试")
    class ExecutionCountTest {

        @Test
        @DisplayName("attemptCount 应在每次进入 RUNNING 时递增")
        void testAttemptCountIncrement() {
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();

            assertEquals(0, lifecycle.getAttemptCount());

            lifecycle.transitionToRunning();
            assertEquals(1, lifecycle.getAttemptCount());

            lifecycle.transitionToRetryWait(3);
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            lifecycle.transitionToRunning();
            assertEquals(2, lifecycle.getAttemptCount());
        }

        @Test
        @DisplayName("retryCount 应在每次进入 RETRY_WAIT 时递增")
        void testRetryCountIncrement() {
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            lifecycle.transitionToRunning();

            assertEquals(0, lifecycle.getRetryCount());

            lifecycle.transitionToRetryWait(3);
            assertEquals(1, lifecycle.getRetryCount());

            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            lifecycle.transitionToRunning();
            lifecycle.transitionToRetryWait(3);
            assertEquals(2, lifecycle.getRetryCount());
        }
    }

    @Nested
    @DisplayName("时间戳记录测试")
    class TimestampTest {

        @Test
        @DisplayName("各状态转换应正确记录时间戳")
        void testTimestampsRecorded() {
            long beforeCreate = System.currentTimeMillis();
            TaskLifecycle lc = new TaskLifecycle("timestamp-test");
            long afterCreate = System.currentTimeMillis();

            assertTrue(lc.getCreateTime() >= beforeCreate);
            assertTrue(lc.getCreateTime() <= afterCreate);

            lc.transitionToScheduled();
            assertTrue(lc.getScheduledTime() >= afterCreate);

            lc.transitionToReady();
            assertTrue(lc.getReadyTime() >= lc.getScheduledTime());

            lc.transitionToRunning();
            assertTrue(lc.getExecutionStartTime() >= lc.getReadyTime());

            lc.transitionToSuccess();
            assertTrue(lc.getCompletionTime() >= lc.getExecutionStartTime());
        }
    }

    @Nested
    @DisplayName("并发安全测试")
    class ConcurrentSafetyTest {

        @Test
        @DisplayName("并发状态转换应保持一致性")
        void testConcurrentTransition() throws InterruptedException {
            int threadCount = 100;
            TaskLifecycle concurrentLifecycle = new TaskLifecycle("concurrent-test");

            // 先进入 SCHEDULED
            concurrentLifecycle.transitionToScheduled();

            // 多线程并发尝试转换到 READY
            Thread[] threads = new Thread[threadCount];
            java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(() -> {
                    if (concurrentLifecycle.transitionToReady()) {
                        successCount.incrementAndGet();
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            // 只有一个线程能成功
            assertEquals(1, successCount.get());
            assertEquals(TaskStatus.READY, concurrentLifecycle.getStatus());
        }
    }

    @Nested
    @DisplayName("状态摘要测试")
    class SummaryTest {

        @Test
        @DisplayName("getSummary 应返回正确摘要")
        void testGetSummary() {
            lifecycle.transitionToScheduled();
            lifecycle.transitionToReady();
            lifecycle.transitionToRunning();

            TaskLifecycle.LifecycleSummary summary = lifecycle.getSummary();

            assertEquals(TEST_TASK_ID, summary.taskId());
            assertEquals(TaskStatus.RUNNING, summary.status());
            assertEquals(1, summary.attemptCount());
            assertEquals(0, summary.retryCount());
            assertTrue(summary.createTime() > 0);
            assertTrue(summary.scheduledTime() > 0);
            assertTrue(summary.readyTime() > 0);
            assertTrue(summary.executionStartTime() > 0);
        }
    }
}
