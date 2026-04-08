package com.loomq.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.entity.TaskStatus;
import com.loomq.entity.Task;
import com.loomq.store.TaskStore;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Webhook 分发器 V3
 *
 * 基于统一状态机的分发器实现。
 *
 * 状态流转（V3）：
 * RUNNING → SUCCESS (HTTP 2xx)
 * RUNNING → RETRY_WAIT (可重试错误)
 * RUNNING → FAILED (不可重试错误)
 * RUNNING → DEAD_LETTER (重试耗尽)
 *
 * 错误分类：
 * - HTTP 4xx (非 429): 不可重试 → FAILED
 * - HTTP 429: 可重试 → RETRY_WAIT
 * - HTTP 5xx: 可重试 → RETRY_WAIT
 * - 网络超时: 可重试 → RETRY_WAIT
 * - 网络错误: 可重试 → RETRY_WAIT
 *
 * @author loomq
 * @since v0.4
 */
public class WebhookDispatcher implements TaskDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(WebhookDispatcher.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final TaskStore taskStore;
    private final WalWriter walWriter;
    private final OkHttpClient httpClient;

    private volatile boolean stopped = false;

    // 配置
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int httpTimeoutMs;

    public WebhookDispatcher(TaskStore taskStore, WalWriter walWriter,
                                int connectTimeoutMs, int readTimeoutMs, int httpTimeoutMs) {
        this.taskStore = taskStore;
        this.walWriter = walWriter;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.httpTimeoutMs = httpTimeoutMs;

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                .readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS)
                .writeTimeout(httpTimeoutMs, TimeUnit.MILLISECONDS)
                .build();

        logger.info("WebhookDispatcher initialized, connectTimeout={}ms, readTimeout={}ms",
                connectTimeoutMs, readTimeoutMs);
    }

    /**
     * 分发任务
     */
    public void dispatch(Task task) {
        if (stopped) {
            logger.warn("Dispatcher is stopped, cannot dispatch task: {}", task.getTaskId());
            return;
        }

        String taskId = task.getTaskId();

        // 二次状态检查
        if (task.getStatus() != TaskStatus.RUNNING) {
            logger.info("Task {} skipped in dispatcher due to status: {}", taskId, task.getStatus());
            return;
        }

        long startTime = System.currentTimeMillis();
        MetricsCollector.getInstance().incrementWebhookRequests();

        try {
            logger.debug("Dispatching task: {} to {}", taskId, task.getWebhookUrl());

            Request request = buildRequest(task);

            try (Response response = httpClient.newCall(request).execute()) {
                long duration = System.currentTimeMillis() - startTime;

                if (response.isSuccessful()) {
                    handleSuccess(task, duration);
                } else {
                    MetricsCollector.getInstance().incrementWebhookError();
                    handleHttpError(task, response.code(), response.message(), duration);
                }
            }
        } catch (java.net.SocketTimeoutException e) {
            MetricsCollector.getInstance().incrementWebhookTimeout();
            handleRetryableError(task, "Timeout: " + e.getMessage(), e);
        } catch (IOException e) {
            MetricsCollector.getInstance().incrementWebhookError();
            handleRetryableError(task, "IO error: " + e.getMessage(), e);
        } catch (Exception e) {
            MetricsCollector.getInstance().incrementWebhookError();
            handleRetryableError(task, "Unexpected error: " + e.getMessage(), e);
        }
    }

    /**
     * 构建 HTTP 请求
     */
    private Request buildRequest(Task task) {
        String method = task.getMethod() != null ? task.getMethod().toUpperCase() : "POST";

        Request.Builder builder = new Request.Builder()
                .url(task.getWebhookUrl());

        // 添加自定义 headers
        if (task.getHeaders() != null) {
            for (Map.Entry<String, String> entry : task.getHeaders().entrySet()) {
                builder.addHeader(entry.getKey(), entry.getValue());
            }
        }

        // 添加标准 headers
        builder.addHeader("X-Loomq-Task-Id", task.getTaskId());
        if (task.getBizKey() != null) {
            builder.addHeader("X-Loomq-Biz-Key", task.getBizKey());
        }
        builder.addHeader("X-Loomq-Wake-Time", String.valueOf(task.getWakeTime()));

        // 请求体
        RequestBody body = null;
        if (task.getPayload() != null && !task.getPayload().isEmpty()) {
            body = RequestBody.create(task.getPayload(), MediaType.parse("application/json"));
        }

        switch (method) {
            case "GET":
                return builder.get().build();
            case "POST":
                return builder.post(body != null ? body : RequestBody.create("", null)).build();
            case "PUT":
                return builder.put(body != null ? body : RequestBody.create("", null)).build();
            case "DELETE":
                return builder.delete(body).build();
            case "PATCH":
                return builder.patch(body != null ? body : RequestBody.create("", null)).build();
            default:
                return builder.post(body != null ? body : RequestBody.create("", null)).build();
        }
    }

    /**
     * 处理成功
     *
     * 状态流转：RUNNING → SUCCESS
     */
    private void handleSuccess(Task task, long duration) {
        String taskId = task.getTaskId();

        MetricsCollector.getInstance().recordWebhookLatency(duration);

        // 状态转换：RUNNING → SUCCESS
        if (task.transitionToSuccess()) {
            taskStore.updateStatus(taskId, TaskStatus.RUNNING, TaskStatus.SUCCESS);

            // 写入 WAL
            walWriter.writeAckEvent(taskId);

            MetricsCollector.getInstance().incrementTasksAckSuccess();

            auditLogger.info("TASK_SUCCESS|{}|{}|{}|{}ms", taskId, task.getBizKey(), task.getWebhookUrl(), duration);

            logger.info("Task completed successfully: {}, duration={}ms", taskId, duration);
        }
    }

    /**
     * 处理 HTTP 错误
     *
     * 根据 HTTP 状态码决定是重试还是直接失败
     */
    private void handleHttpError(Task task, int httpCode, String message, long duration) {
        String error = "HTTP " + httpCode + ": " + message;
        logger.warn("Task {} HTTP error: {}", task.getTaskId(), error);

        // 错误分类
        if (httpCode >= 400 && httpCode < 500 && httpCode != 429) {
            // HTTP 4xx (非 429): 不可重试 → FAILED
            handleNonRetryableError(task, error);
        } else {
            // HTTP 429, 5xx: 可重试 → RETRY_WAIT
            handleRetryableError(task, error, null);
        }
    }

    /**
     * 处理可重试错误
     *
     * 状态流转：RUNNING → RETRY_WAIT 或 DEAD_LETTER
     */
    private void handleRetryableError(Task task, String error, Exception exception) {
        String taskId = task.getTaskId();
        int maxRetry = task.getMaxRetryCount();

        logger.warn("Task {} retryable error: {}, retryCount={}/{}",
                taskId, error, task.getRetryCount(), maxRetry);

        // 状态转换：RUNNING → RETRY_WAIT 或 DEAD_LETTER
        if (task.transitionToRetryWait()) {
            TaskStatus newStatus = task.getStatus();

            if (newStatus == TaskStatus.DEAD_LETTER) {
                // 重试耗尽，进入死信
                taskStore.updateStatus(taskId, TaskStatus.RUNNING, TaskStatus.DEAD_LETTER);
                walWriter.writeDeadLetterEvent(taskId, error);
                MetricsCollector.getInstance().incrementTasksDeadLetter();
                auditLogger.warn("TASK_DEAD_LETTER|{}|{}|{}", taskId, task.getBizKey(), error);
                logger.error("Task entered DEAD_LETTER: {}, error: {}", taskId, error);
            } else {
                // 进入重试等待
                taskStore.updateStatus(taskId, TaskStatus.RUNNING, TaskStatus.RETRY_WAIT);
                walWriter.writeRetryEvent(taskId, task.getRetryCount(), error);
                MetricsCollector.getInstance().incrementTasksRetry();
                auditLogger.info("TASK_RETRY|{}|{}|retry#{}|{}", taskId, task.getBizKey(), task.getRetryCount(), error);
                logger.info("Task scheduled for retry: {}, retryCount={}", taskId, task.getRetryCount());
            }
        }
    }

    /**
     * 处理不可重试错误
     *
     * 状态流转：RUNNING → FAILED
     */
    private void handleNonRetryableError(Task task, String error) {
        String taskId = task.getTaskId();

        // 状态转换：RUNNING → FAILED
        if (task.transitionToFailed(error)) {
            taskStore.updateStatus(taskId, TaskStatus.RUNNING, TaskStatus.FAILED);
            walWriter.writeFailEvent(taskId, error);
            MetricsCollector.getInstance().incrementTasksFailedTerminal();
            auditLogger.warn("TASK_FAILED|{}|{}|{}", taskId, task.getBizKey(), error);
            logger.error("Task FAILED (non-retryable): {}, error: {}", taskId, error);
        }
    }

    /**
     * 停止分发器
     */
    public void stop() {
        stopped = true;
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
        logger.info("WebhookDispatcher stopped");
    }
}
