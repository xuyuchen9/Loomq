package com.loomq.gateway;

import com.loomq.common.AlertService;
import com.loomq.common.MetricsCollector;
import com.loomq.store.TaskStore;
import io.javalin.http.Context;

import java.util.HashMap;
import java.util.Map;

/**
 * 健康检查控制器
 */
public class HealthController {
    private final TaskStore taskStore;
    private final MetricsCollector metrics;

    public HealthController(TaskStore taskStore) {
        this.taskStore = taskStore;
        this.metrics = MetricsCollector.getInstance();
    }

    /**
     * 健康检查
     * GET /health
     */
    public void health(Context ctx) {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("timestamp", System.currentTimeMillis());
        health.put("taskStats", taskStore.getStats());
        health.put("metrics", Map.of(
            "trigger_latency_ms_p95", metrics.getP95LatencyMs(),
            "webhook_timeout_rate_percent", String.format("%.2f", metrics.getWebhookTimeoutRate()),
            "wal_size_bytes", metrics.getWalSizeBytes()
        ));
        health.put("alerts", AlertService.getInstance().getStatus());

        ctx.json(health);
    }

    /**
     * 指标
     * GET /metrics
     */
    public void metrics(Context ctx) {
        Map<String, Long> stats = taskStore.getStats();
        String prometheusMetrics = metrics.exportPrometheusMetrics(stats);
        ctx.contentType("text/plain; version=0.0.4").result(prometheusMetrics);
    }
}
