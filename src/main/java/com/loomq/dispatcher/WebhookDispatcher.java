package com.loomq.dispatcher;

import com.loomq.common.IdGenerator;
import com.loomq.common.MetricsCollector;
import com.loomq.config.DispatcherConfig;
import com.loomq.config.RetryConfig;
import com.loomq.entity.EventType;
import com.loomq.entity.Task;
import com.loomq.entity.TaskEvent;
import com.loomq.entity.TaskStatus;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalEngine;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Webhook 分发器
 * 发起 webhook 请求，处理超时和重试
 */
public class WebhookDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(WebhookDispatcher.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final WalEngine walEngine;
    private final TaskStore taskStore;
    private final RetryPolicy retryPolicy;
    private final OkHttpClient httpClient;

    private volatile boolean stopped = false;

    public WebhookDispatcher(WalEngine walEngine, TaskStore taskStore,
                              DispatcherConfig config, RetryConfig retryConfig) {
        this.walEngine = walEngine;
        this.taskStore = taskStore;
        this.retryPolicy = new RetryPolicy(retryConfig);

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(config.connectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.httpTimeoutMs(), TimeUnit.MILLISECONDS)
                .build();

        logger.info("Webhook dispatcher initialized, connectTimeout={}ms, readTimeout={}ms",
                config.connectTimeoutMs(), config.readTimeoutMs());
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

        // 二次状态检查（防御性编程）
        // 即使调度器已检查，仍可能存在竞态，此处再次确认
        if (!task.canExecute()) {
            logger.info("Task {} skipped in dispatcher due to status: {}",
                    taskId, task.getStatus());
            return;
        }

        long startTime = System.currentTimeMillis();

        // 记录执行开始时间
        task.setExecutionStartTime(startTime);

        // 记录 webhook 请求
        MetricsCollector.getInstance().incrementWebhookRequests();

        try {
            logger.debug("Dispatching task: {} to {}", taskId, task.getWebhookUrl());

            // 构建请求
            Request request = buildRequest(task);

            // 发送请求
            try (Response response = httpClient.newCall(request).execute()) {
                long duration = System.currentTimeMillis() - startTime;

                if (response.isSuccessful()) {
                    handleSuccess(task, duration);
                } else {
                    MetricsCollector.getInstance().incrementWebhookError();
                    handleFailure(task, "HTTP " + response.code() + ": " + response.message(), null);
                }
            }
        } catch (java.net.SocketTimeoutException e) {
            MetricsCollector.getInstance().incrementWebhookTimeout();
            handleFailure(task, "Timeout: " + e.getMessage(), e);
        } catch (IOException e) {
            MetricsCollector.getInstance().incrementWebhookError();
            handleFailure(task, "IO error: " + e.getMessage(), e);
        } catch (Exception e) {
            MetricsCollector.getInstance().incrementWebhookError();
            handleFailure(task, "Unexpected error: " + e.getMessage(), e);
        }
    }

    /**
     * 构建HTTP请求
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
        builder.addHeader("X-Loomq-Trigger-Time", String.valueOf(task.getTriggerTime()));
        builder.addHeader("X-Loomq-Retry-Count", String.valueOf(task.getRetryCount()));

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
     */
    private void handleSuccess(Task task, long duration) {
        String taskId = task.getTaskId();

        // 记录 webhook 延迟 (执行 → 响应)
        MetricsCollector.getInstance().recordWebhookLatency(duration);

        // 记录总延迟 (计划时间 → 完成)
        long totalLatency = System.currentTimeMillis() - task.getTriggerTime();
        MetricsCollector.getInstance().recordTotalLatency(totalLatency);

        try {
            // 更新状态为 ACKED
            TaskStatus oldStatus = task.getStatus();
            task.setStatus(TaskStatus.ACKED);
            task.incrementVersion();
            taskStore.updateStatus(task, oldStatus);

            // 写入 ACK 事件
            TaskEvent ackEvent = TaskEvent.builder()
                    .eventId(IdGenerator.generateEventId())
                    .taskId(taskId)
                    .eventType(EventType.ACK)
                    .version(task.getVersion())
                    .prevVersion(task.getVersion() - 1)
                    .eventTime(System.currentTimeMillis())
                    .payload("{\"duration_ms\":" + duration + "}")
                    .build();
            walEngine.appendEvent(ackEvent);

            // 记录审计日志
            auditLogger.info("TASK_ACK|{}|{}|{}|{}ms", taskId, task.getBizKey(), task.getWebhookUrl(), duration);

            // 记录指标
            MetricsCollector.getInstance().incrementTasksAckSuccess();

            logger.info("Task completed successfully: {}, duration={}ms", taskId, duration);
        } catch (IOException e) {
            logger.error("Failed to write ACK event for task: {}", taskId, e);
        }
    }

    /**
     * 处理失败
     */
    private void handleFailure(Task task, String error, Exception exception) {
        String taskId = task.getTaskId();

        logger.warn("Task dispatch failed: {}, error: {}", taskId, error);
        if (exception != null) {
            logger.debug("Exception details", exception);
        }

        task.setError(error);

        // 判断是否可以重试
        if (retryPolicy.canRetry(task)) {
            scheduleRetry(task, error);
        } else {
            markTerminalFailure(task, error);
        }
    }

    /**
     * 安排重试
     */
    private void scheduleRetry(Task task, String error) {
        String taskId = task.getTaskId();

        try {
            TaskStatus oldStatus = task.getStatus();
            task.incrementRetry();
            task.setStatus(TaskStatus.RETRY_WAIT);
            long nextTriggerTime = retryPolicy.calculateNextTriggerTime(task);
            task.setTriggerTime(nextTriggerTime);
            task.incrementVersion();
            taskStore.updateStatus(task, oldStatus);

            // 写入 RETRY 事件
            TaskEvent retryEvent = TaskEvent.builder()
                    .eventId(IdGenerator.generateEventId())
                    .taskId(taskId)
                    .eventType(EventType.RETRY)
                    .version(task.getVersion())
                    .prevVersion(task.getVersion() - 1)
                    .eventTime(System.currentTimeMillis())
                    .payload("{\"retry_count\":" + task.getRetryCount() + ",\"error\":\"" + escapeJson(error) + "\"}")
                    .build();
            walEngine.appendEvent(retryEvent);

            // 重新调度
            // 注意：这里需要通过 scheduler 重新调度，暂时先存储更新后的任务
            // 实际应该调用 scheduler.reschedule(task)

            // 记录审计日志
            auditLogger.info("TASK_RETRY|{}|{}|retry#{}|{}", taskId, task.getBizKey(), task.getRetryCount(), error);

            // 记录指标
            MetricsCollector.getInstance().incrementTasksRetry();

            logger.info("Task scheduled for retry: {}, retryCount={}, nextTriggerTime={}",
                    taskId, task.getRetryCount(), nextTriggerTime);
        } catch (IOException e) {
            logger.error("Failed to write RETRY event for task: {}", taskId, e);
        }
    }

    /**
     * 标记为终态失败
     */
    private void markTerminalFailure(Task task, String error) {
        String taskId = task.getTaskId();

        try {
            TaskStatus oldStatus = task.getStatus();
            task.setStatus(TaskStatus.FAILED_TERMINAL);
            task.setError(error);
            task.incrementVersion();
            taskStore.updateStatus(task, oldStatus);

            // 写入 FAIL 事件
            TaskEvent failEvent = TaskEvent.builder()
                    .eventId(IdGenerator.generateEventId())
                    .taskId(taskId)
                    .eventType(EventType.FAIL)
                    .version(task.getVersion())
                    .prevVersion(task.getVersion() - 1)
                    .eventTime(System.currentTimeMillis())
                    .payload("{\"error\":\"" + escapeJson(error) + "\"}")
                    .build();
            walEngine.appendEvent(failEvent);

            // 记录审计日志
            auditLogger.info("TASK_FAILED|{}|{}|{}", taskId, task.getBizKey(), error);

            // 记录指标
            MetricsCollector.getInstance().incrementTasksFailedTerminal();

            logger.error("Task failed terminally: {}, error: {}", taskId, error);
        } catch (IOException e) {
            logger.error("Failed to write FAIL event for task: {}", taskId, e);
        }
    }

    /**
     * 停止分发器
     */
    public void stop() {
        stopped = true;
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
        logger.info("Webhook dispatcher stopped");
    }

    /**
     * 转义 JSON 字符串
     */
    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
