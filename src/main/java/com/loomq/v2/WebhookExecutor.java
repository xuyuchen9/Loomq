package com.loomq.v2;

import com.loomq.entity.Task;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Webhook 执行器
 */
public class WebhookExecutor {

    private static final Logger logger = LoggerFactory.getLogger(WebhookExecutor.class);

    private final OkHttpClient httpClient;
    private final LoomqConfigV2 config;

    public WebhookExecutor(LoomqConfigV2 config) {
        this.config = config;

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(config.webhookConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.webhookReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * 执行 webhook
     */
    public ExecuteResult execute(Task task) {
        long startTime = System.currentTimeMillis();

        try {
            Request request = buildRequest(task);

            try (Response response = httpClient.newCall(request).execute()) {
                long duration = System.currentTimeMillis() - startTime;

                if (response.isSuccessful()) {
                    logger.debug("Task {} webhook success: {}ms", task.getTaskId(), duration);
                    return ExecuteResult.success(duration);
                } else {
                    logger.warn("Task {} webhook failed: HTTP {}", task.getTaskId(), response.code());
                    return ExecuteResult.retry("HTTP " + response.code());
                }
            }

        } catch (java.net.SocketTimeoutException e) {
            logger.warn("Task {} webhook timeout", task.getTaskId());
            return ExecuteResult.retry("Timeout");

        } catch (IOException e) {
            logger.error("Task {} webhook error: {}", task.getTaskId(), e.getMessage());
            return ExecuteResult.retry(e.getMessage());

        } catch (Exception e) {
            logger.error("Task {} webhook unexpected error", task.getTaskId(), e);
            return ExecuteResult.dead(e.getMessage());
        }
    }

    private Request buildRequest(Task task) {
        String method = task.getMethod() != null ? task.getMethod().toUpperCase() : "POST";

        Request.Builder builder = new Request.Builder()
                .url(task.getWebhookUrl());

        // 添加 headers
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
            default:
                return builder.post(body != null ? body : RequestBody.create("", null)).build();
        }
    }

    /**
     * 执行结果
     */
    public record ExecuteResult(
            Status status,
            long durationMs,
            String error
    ) {
        public enum Status {
            SUCCESS,  // 成功
            RETRY,    // 可重试
            DEAD      // 死信
        }

        public static ExecuteResult success(long durationMs) {
            return new ExecuteResult(Status.SUCCESS, durationMs, null);
        }

        public static ExecuteResult retry(String error) {
            return new ExecuteResult(Status.RETRY, 0, error);
        }

        public static ExecuteResult dead(String error) {
            return new ExecuteResult(Status.DEAD, 0, error);
        }
    }
}
