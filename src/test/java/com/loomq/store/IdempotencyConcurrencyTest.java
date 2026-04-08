package com.loomq.store;

import com.loomq.entity.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 幂等并发测试
 *
 * 验证幂等创建在高并发场景下的正确性。
 *
 * @author loomq
 * @since v0.4.1
 */
@DisplayName("幂等并发测试")
class IdempotencyConcurrencyTest {

    private TaskStore taskStore;

    @BeforeEach
    void setUp() {
        taskStore = new TaskStore();
    }

    @Test
    @DisplayName("单线程幂等创建应正常工作")
    void testSingleThreadIdempotency() {
        String idempotencyKey = "single-thread-key";

        Task task1 = Task.builder()
                .taskId("task-1")
                .idempotencyKey(idempotencyKey)
                .webhookUrl("https://example.com/webhook")
                .wakeTime(System.currentTimeMillis() + 3600000)
                .build();

        IdempotencyResult result1 = taskStore.addWithIdempotency(task1);
        assertTrue(result1.isNotFound(), "第一次创建应成功");

        Task task2 = Task.builder()
                .taskId("task-2")
                .idempotencyKey(idempotencyKey)
                .webhookUrl("https://example.com/webhook")
                .wakeTime(System.currentTimeMillis() + 3600000)
                .build();

        IdempotencyResult result2 = taskStore.addWithIdempotency(task2);
        assertTrue(result2.exists(), "第二次创建应返回已存在");
        assertEquals("task-1", result2.getTask().getTaskId());
    }

    @RepeatedTest(10)
    @DisplayName("并发幂等创建：10线程竞争同一幂等键，只创建一个任务")
    void testConcurrentIdempotency10Threads() throws InterruptedException {
        testConcurrentIdempotency(10, "idem-key-10-" + System.nanoTime());
    }

    @RepeatedTest(5)
    @DisplayName("并发幂等创建：50线程竞争同一幂等键，只创建一个任务")
    void testConcurrentIdempotency50Threads() throws InterruptedException {
        testConcurrentIdempotency(50, "idem-key-50-" + System.nanoTime());
    }

    @RepeatedTest(3)
    @DisplayName("并发幂等创建：100线程竞争同一幂等键，只创建一个任务")
    void testConcurrentIdempotency100Threads() throws InterruptedException {
        testConcurrentIdempotency(100, "idem-key-100-" + System.nanoTime());
    }

    void testConcurrentIdempotency(int threadCount, String idempotencyKey) throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger existsCount = new AtomicInteger(0);
        ConcurrentHashMap<String, String> taskIdMap = new ConcurrentHashMap<>();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();

                    Task task = Task.builder()
                            .taskId("task-" + index + "-" + System.nanoTime())
                            .idempotencyKey(idempotencyKey)
                            .webhookUrl("https://example.com/webhook")
                            .wakeTime(System.currentTimeMillis() + 3600000)
                            .build();

                    IdempotencyResult result = taskStore.addWithIdempotency(task);

                    if (result.isNotFound()) {
                        successCount.incrementAndGet();
                        taskIdMap.put("success", task.getTaskId());
                    } else {
                        existsCount.incrementAndGet();
                        taskIdMap.put("exists", result.getTask().getTaskId());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "所有线程应在30秒内完成");
        executor.shutdown();

        // 验证结果
        assertEquals(1, successCount.get(),
                "只有一个线程应该成功创建任务，但成功了 " + successCount.get() + " 个");
        assertEquals(threadCount - 1, existsCount.get(),
                "其他线程应该返回已存在");

        // 验证所有线程看到的任务ID一致
        String successTaskId = taskIdMap.get("success");
        String existsTaskId = taskIdMap.get("exists");
        if (successTaskId != null && existsTaskId != null) {
            assertEquals(successTaskId, existsTaskId,
                    "所有线程应该看到同一个任务ID");
        }

        // 验证存储中只有一个任务
        assertEquals(1, taskStore.size(), "存储中应该只有一个任务");
    }

    @Test
    @DisplayName("并发创建不同幂等键的任务应全部成功")
    void testConcurrentDifferentKeys() throws InterruptedException {
        int taskCount = 1000;
        CountDownLatch latch = new CountDownLatch(taskCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(50);

        for (int i = 0; i < taskCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    Task task = Task.builder()
                            .taskId("task-" + index)
                            .idempotencyKey("unique-key-" + index)
                            .webhookUrl("https://example.com/webhook")
                            .wakeTime(System.currentTimeMillis() + 3600000)
                            .build();

                    IdempotencyResult result = taskStore.addWithIdempotency(task);
                    if (result.isNotFound()) {
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(taskCount, successCount.get(),
                "所有不同幂等键的创建都应该成功");
        assertEquals(0, failureCount.get());
        assertEquals(taskCount, taskStore.size());
    }

    @Test
    @DisplayName("终态幂等键保护：已终态任务的幂等键不能复用")
    void testTerminalIdempotencyProtection() {
        String idempotencyKey = "terminal-protection-key";

        // 创建任务并使其进入终态
        Task task = Task.builder()
                .taskId("task-terminal")
                .idempotencyKey(idempotencyKey)
                .webhookUrl("https://example.com/webhook")
                .wakeTime(System.currentTimeMillis())
                .build();

        taskStore.addWithIdempotency(task);
        task.transitionToScheduled();
        task.transitionToReady();
        task.transitionToRunning();
        task.transitionToSuccess();

        // 更新存储中的状态
        taskStore.update(task);

        // 尝试用相同幂等键创建新任务
        Task newTask = Task.builder()
                .taskId("task-new")
                .idempotencyKey(idempotencyKey)
                .webhookUrl("https://example.com/webhook")
                .wakeTime(System.currentTimeMillis() + 3600000)
                .build();

        IdempotencyResult result = taskStore.addWithIdempotency(newTask);
        assertTrue(result.isTerminal(), "应该返回终态存在");
        assertNotNull(result.getTask());
        assertTrue(result.getTask().getStatus().isTerminal());
    }

    @Test
    @DisplayName("混合场景：并发创建包含重复和唯一键")
    void testMixedConcurrentScenario() throws InterruptedException {
        int uniqueCount = 100;
        int duplicateGroups = 10;
        int duplicatesPerGroup = 10;

        CountDownLatch latch = new CountDownLatch(uniqueCount + duplicateGroups * duplicatesPerGroup);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger duplicateCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(50);

        // 创建唯一键任务
        for (int i = 0; i < uniqueCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    Task task = Task.builder()
                            .taskId("unique-task-" + index)
                            .idempotencyKey("unique-key-" + index)
                            .webhookUrl("https://example.com/webhook")
                            .wakeTime(System.currentTimeMillis() + 3600000)
                            .build();

                    IdempotencyResult result = taskStore.addWithIdempotency(task);
                    if (result.isNotFound()) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // 创建重复键任务（每组 duplicatesPerGroup 个线程竞争同一个键）
        for (int g = 0; g < duplicateGroups; g++) {
            final int groupId = g;
            final String sharedKey = "shared-key-" + groupId;
            for (int d = 0; d < duplicatesPerGroup; d++) {
                final int index = d;
                executor.submit(() -> {
                    try {
                        Task task = Task.builder()
                                .taskId("dup-task-" + groupId + "-" + index)
                                .idempotencyKey(sharedKey)
                                .webhookUrl("https://example.com/webhook")
                                .wakeTime(System.currentTimeMillis() + 3600000)
                                .build();

                        IdempotencyResult result = taskStore.addWithIdempotency(task);
                        if (result.isNotFound()) {
                            successCount.incrementAndGet();
                        } else {
                            duplicateCount.incrementAndGet();
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        // 验证：唯一键全部成功 + 每组重复键只有一个成功
        int expectedSuccess = uniqueCount + duplicateGroups;
        assertEquals(expectedSuccess, successCount.get(),
                "应该有 " + expectedSuccess + " 个任务成功创建");
        assertEquals(duplicateGroups * (duplicatesPerGroup - 1), duplicateCount.get(),
                "应该有 " + (duplicateGroups * (duplicatesPerGroup - 1)) + " 个重复请求");
        assertEquals(expectedSuccess, taskStore.size(),
                "存储中应该有 " + expectedSuccess + " 个任务");
    }

    @Test
    @DisplayName("索引一致性：并发操作后所有索引保持一致")
    void testIndexConsistencyAfterConcurrentOps() throws InterruptedException {
        int taskCount = 500;
        CountDownLatch latch = new CountDownLatch(taskCount);

        ExecutorService executor = Executors.newFixedThreadPool(50);

        for (int i = 0; i < taskCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    Task task = Task.builder()
                            .taskId("consistency-task-" + index)
                            .idempotencyKey("consistency-idem-" + index)
                            .bizKey("consistency-biz-" + index)
                            .webhookUrl("https://example.com/webhook")
                            .wakeTime(System.currentTimeMillis() + index * 1000)
                            .build();

                    taskStore.addWithIdempotency(task);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        // 验证所有索引一致
        for (int i = 0; i < taskCount; i++) {
            final String taskId = "consistency-task-" + i;
            final String idemKey = "consistency-idem-" + i;
            final String bizKey = "consistency-biz-" + i;

            // 主索引
            Task byId = taskStore.get(taskId);
            assertNotNull(byId, "主索引应包含任务 " + taskId);

            // 幂等索引
            IdempotencyResult byIdem = taskStore.getByIdempotencyKey(idemKey);
            assertTrue(byIdem.exists(), "幂等索引应包含键 " + idemKey);
            assertEquals(taskId, byIdem.getTask().getTaskId(), "幂等索引应指向正确任务");

            // 业务键索引
            Task byBiz = taskStore.getByBizKey(bizKey);
            assertNotNull(byBiz, "业务键索引应包含键 " + bizKey);
            assertEquals(taskId, byBiz.getTaskId(), "业务键索引应指向正确任务");
        }

        assertEquals(taskCount, taskStore.size());
    }
}
