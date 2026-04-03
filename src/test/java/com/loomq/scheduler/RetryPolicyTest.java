package com.loomq.scheduler;

import com.loomq.dispatcher.RetryPolicy;
import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {

    private RetryPolicy policy;
    private TestRetryConfig config;

    @BeforeEach
    void setUp() {
        config = new TestRetryConfig();
        policy = new RetryPolicy(config);
    }

    @Test
    void testCalculateInitialDelay() {
        assertEquals(1000, policy.calculateDelay(0));
    }

    @Test
    void testExponentialBackoff() {
        // Initial: 1000
        // After 1 retry: 2000
        // After 2 retries: 4000
        // After 3 retries: 8000
        assertEquals(1000, policy.calculateDelay(0));
        assertEquals(2000, policy.calculateDelay(1));
        assertEquals(4000, policy.calculateDelay(2));
        assertEquals(8000, policy.calculateDelay(3));
    }

    @Test
    void testMaxDelay() {
        // With multiplier 2.0, after several retries we should hit max
        long delay = policy.calculateDelay(10);
        assertTrue(delay <= config.maxDelayMs());
        assertEquals(60000, delay);
    }

    @Test
    void testCanRetry() {
        Task task = createTestTask(0, 5);
        assertTrue(policy.canRetry(task));

        task.setRetryCount(4);
        assertTrue(policy.canRetry(task));

        task.setRetryCount(5);
        assertFalse(policy.canRetry(task));
    }

    @Test
    void testCannotRetryTerminalStatus() {
        Task task = createTestTask(0, 5);
        assertTrue(policy.canRetry(task));

        task.setStatus(TaskStatus.CANCELLED);
        assertFalse(policy.canRetry(task));

        task.setStatus(TaskStatus.ACKED);
        assertFalse(policy.canRetry(task));
    }

    @Test
    void testCalculateNextTriggerTime() {
        Task task = createTestTask(2, 5);
        long nextTime = policy.calculateNextTriggerTime(task);

        long expectedDelay = policy.calculateDelay(2);
        assertTrue(nextTime > System.currentTimeMillis());
        assertTrue(nextTime <= System.currentTimeMillis() + expectedDelay + 100);
    }

    private Task createTestTask(int retryCount, int maxRetry) {
        return Task.builder()
                .taskId("t_001")
                .status(TaskStatus.RETRY_WAIT)
                .triggerTime(System.currentTimeMillis())
                .webhookUrl("https://example.com/webhook")
                .retryCount(retryCount)
                .maxRetry(maxRetry)
                .build();
    }

    private static class TestRetryConfig implements com.loomq.config.RetryConfig {
        @Override
        public long initialDelayMs() {
            return 1000;
        }

        @Override
        public long maxDelayMs() {
            return 60000;
        }

        @Override
        public double multiplier() {
            return 2.0;
        }

        @Override
        public int defaultMaxRetry() {
            return 5;
        }
    }
}
