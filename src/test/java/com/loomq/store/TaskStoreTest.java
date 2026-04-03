package com.loomq.store;

import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TaskStoreTest {

    private TaskStore store;

    @BeforeEach
    void setUp() {
        store = new TaskStore();
    }

    @Test
    void testAddTask() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        store.add(task);

        assertEquals(1, store.size());
        assertNotNull(store.get("t_001"));
    }

    @Test
    void testAddDuplicateTask() {
        Task task1 = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        store.add(task1);

        Task task2 = createTestTask("t_001", "biz_002", TaskStatus.PENDING);
        assertThrows(IllegalStateException.class, () -> store.add(task2));
    }

    @Test
    void testUpdateTask() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        store.add(task);

        task.setStatus(TaskStatus.SCHEDULED);
        store.update(task);

        Task updated = store.get("t_001");
        assertEquals(TaskStatus.SCHEDULED, updated.getStatus());
    }

    @Test
    void testUpdateNonExistentTask() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        assertThrows(java.util.NoSuchElementException.class, () -> store.update(task));
    }

    @Test
    void testRemoveTask() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        store.add(task);

        Task removed = store.remove("t_001");

        assertNotNull(removed);
        assertEquals("t_001", removed.getTaskId());
        assertNull(store.get("t_001"));
        assertEquals(0, store.size());
    }

    @Test
    void testGetByIdempotencyKey() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        task.setIdempotencyKey("idem_001");
        store.add(task);

        Task found = store.getByIdempotencyKey("idem_001");
        assertNotNull(found);
        assertEquals("t_001", found.getTaskId());

        assertNull(store.getByIdempotencyKey("nonexistent"));
    }

    @Test
    void testGetByBizKey() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        store.add(task);

        Task found = store.getByBizKey("biz_001");
        assertNotNull(found);
        assertEquals("t_001", found.getTaskId());

        assertNull(store.getByBizKey("nonexistent"));
    }

    @Test
    void testExistsByIdempotencyKey() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        task.setIdempotencyKey("idem_001");
        store.add(task);

        assertTrue(store.existsByIdempotencyKey("idem_001"));
        assertFalse(store.existsByIdempotencyKey("nonexistent"));
    }

    @Test
    void testExistsByBizKey() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        store.add(task);

        assertTrue(store.existsByBizKey("biz_001"));
        assertFalse(store.existsByBizKey("nonexistent"));
    }

    @Test
    void testGetDueTasks() {
        long now = System.currentTimeMillis();

        Task task1 = createTestTask("t_001", "biz_001", TaskStatus.SCHEDULED);
        task1.setTriggerTime(now - 1000); // overdue

        Task task2 = createTestTask("t_002", "biz_002", TaskStatus.SCHEDULED);
        task2.setTriggerTime(now + 10000); // future

        Task task3 = createTestTask("t_003", "biz_003", TaskStatus.PENDING);
        task3.setTriggerTime(now - 2000); // overdue but not scheduled

        store.add(task1);
        store.add(task2);
        store.add(task3);

        List<Task> dueTasks = store.getDueTasks(now);

        assertEquals(1, dueTasks.size());
        assertEquals("t_001", dueTasks.get(0).getTaskId());
    }

    @Test
    void testCountByStatus() {
        store.add(createTestTask("t_001", "biz_001", TaskStatus.PENDING));
        store.add(createTestTask("t_002", "biz_002", TaskStatus.SCHEDULED));
        store.add(createTestTask("t_003", "biz_003", TaskStatus.SCHEDULED));
        store.add(createTestTask("t_004", "biz_004", TaskStatus.ACKED));

        assertEquals(1, store.countByStatus(TaskStatus.PENDING));
        assertEquals(2, store.countByStatus(TaskStatus.SCHEDULED));
        assertEquals(1, store.countByStatus(TaskStatus.ACKED));
        assertEquals(0, store.countByStatus(TaskStatus.CANCELLED));
    }

    @Test
    void testGetStats() {
        store.add(createTestTask("t_001", "biz_001", TaskStatus.PENDING));
        store.add(createTestTask("t_002", "biz_002", TaskStatus.SCHEDULED));
        store.add(createTestTask("t_003", "biz_003", TaskStatus.ACKED));

        Map<String, Long> stats = store.getStats();

        assertEquals(3L, stats.get("total"));
        assertEquals(1L, stats.get("pending"));
        assertEquals(1L, stats.get("scheduled"));
        assertEquals(1L, stats.get("acked"));
    }

    @Test
    void testClear() {
        store.add(createTestTask("t_001", "biz_001", TaskStatus.PENDING));
        store.add(createTestTask("t_002", "biz_002", TaskStatus.SCHEDULED));

        store.clear();

        assertEquals(0, store.size());
        assertNull(store.get("t_001"));
        assertNull(store.get("t_002"));
    }

    @Test
    void testTimeIndexUpdate() {
        long now = System.currentTimeMillis();
        Task task = createTestTask("t_001", "biz_001", TaskStatus.SCHEDULED);
        task.setTriggerTime(now);
        store.add(task);

        // Get due tasks
        List<Task> dueBefore = store.getDueTasks(now);
        assertEquals(1, dueBefore.size());

        // Remove and add with new trigger time
        store.remove("t_001");
        Task updatedTask = createTestTask("t_001", "biz_001", TaskStatus.SCHEDULED);
        updatedTask.setTriggerTime(now + 100000);
        store.add(updatedTask);

        // Should not be due anymore
        List<Task> dueAfter = store.getDueTasks(now + 1000);
        assertEquals(0, dueAfter.size());
    }

    @Test
    void testStatusIndexUpdate() {
        Task task = createTestTask("t_001", "biz_001", TaskStatus.PENDING);
        store.add(task);

        assertEquals(1, store.countByStatus(TaskStatus.PENDING));
        assertEquals(0, store.countByStatus(TaskStatus.SCHEDULED));

        // Remove and add with new status
        store.remove("t_001");
        Task updatedTask = createTestTask("t_001", "biz_001", TaskStatus.SCHEDULED);
        store.add(updatedTask);

        assertEquals(0, store.countByStatus(TaskStatus.PENDING));
        assertEquals(1, store.countByStatus(TaskStatus.SCHEDULED));
    }

    private Task createTestTask(String taskId, String bizKey, TaskStatus status) {
        return Task.builder()
                .taskId(taskId)
                .bizKey(bizKey)
                .status(status)
                .triggerTime(System.currentTimeMillis() + 60000)
                .webhookUrl("https://example.com/webhook")
                .createTime(System.currentTimeMillis())
                .build();
    }
}
