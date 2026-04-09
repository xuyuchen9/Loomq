package com.loomq;

import com.loomq.common.AlertService;
import com.loomq.common.MetricsCollector;
import com.loomq.config.LoomqConfig;
import com.loomq.entity.Task;
import com.loomq.gateway.TaskController;
import com.loomq.recovery.RecoveryService;
import com.loomq.scheduler.TimeBucketScheduler;
import com.loomq.scheduler.WalWriter;
import com.loomq.scheduler.WebhookDispatcher;
import com.loomq.store.TaskStore;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoomQ V3 启动入口
 *
 * 基于统一状态机的完整实现。
 *
 * 架构：
 * - TaskStore: 统一存储，支持幂等、多索引查询
 * - TaskScheduler: 虚拟线程调度器
 * - WebhookDispatcher: 状态机驱动的分发器
 * - RecoveryService: WAL 恢复服务
 *
 * 状态机（10种状态）：
 * 非终态：PENDING → SCHEDULED → READY → RUNNING → RETRY_WAIT
 * 终态：SUCCESS, FAILED, CANCELLED, EXPIRED, DEAD_LETTER
 *
 * @author loomq
 * @since v0.4
 */
public class LoomqApplication {
    private static final Logger logger = LoggerFactory.getLogger(LoomqApplication.class);

    private final LoomqConfig config;
    private Javalin server;

    // V3 组件
    private TaskStore taskStore;
    private TimeBucketScheduler scheduler;
    private WebhookDispatcher dispatcher;
    private RecoveryService recoveryService;
    private WalWriter walWriter;

    public LoomqApplication() {
        this.config = LoomqConfig.getInstance();
    }

    public static void main(String[] args) {
        LoomqApplication app = new LoomqApplication();
        try {
            app.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down LoomQ V3...");
                app.stop();
                logger.info("LoomQ V3 stopped.");
            }));
        } catch (Exception e) {
            logger.error("Failed to start LoomQ V3", e);
            System.exit(1);
        }
    }

    public void start() throws Exception {
        logger.info("Starting LoomQ V3...");
        logger.info("Using unified state machine with 10 states");

        // 1. 初始化存储层
        logger.info("Initializing TaskStore...");
        taskStore = new TaskStore();

        // 2. 初始化 WAL 写入器
        logger.info("Initializing WalWriter...");
        walWriter = new WalWriter();

        // 3. 初始化分发器
        logger.info("Initializing WebhookDispatcher...");
        dispatcher = new WebhookDispatcher(
                taskStore,
                walWriter,
                5000,   // connectTimeout
                10000,  // readTimeout
                5000    // httpTimeout
        );

        // 4. 初始化调度器（使用 V2 时间桶调度器）
        logger.info("Initializing TimeBucketScheduler...");
        scheduler = new TimeBucketScheduler(dispatcher, 10000);

        // 5. 初始化恢复服务
        logger.info("Initializing RecoveryService...");
        recoveryService = new RecoveryService(
                taskStore,
                RecoveryService.RecoveryConfig.defaultConfig()
        );

        // 6. 执行恢复
        logger.info("Executing recovery...");
        // TODO: 实际恢复逻辑

        // 7. 启动调度器
        scheduler.start();

        // 8. 启动 HTTP 服务
        logger.info("Starting HTTP server...");
        startHttpServer();

        // 启动告警服务
        AlertService.getInstance().start();

        logger.info("LoomQ V3 started successfully on {}:{}",
                config.getServerConfig().host(), config.getServerConfig().port());
    }

    private void startHttpServer() {
        server = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson());
            config.showJavalinBanner = false;
        });

        // 注册路由
        TaskController taskController = new TaskController(taskStore);

        // 任务 API
        server.post("/api/v1/tasks", taskController::createTask);
        server.get("/api/v1/tasks/{taskId}", taskController::getTask);
        server.delete("/api/v1/tasks/{taskId}", taskController::cancelTask);
        server.patch("/api/v1/tasks/{taskId}", taskController::modifyTask);
        server.post("/api/v1/tasks/{taskId}/fire-now", taskController::fireNow);

        // 健康检查
        server.get("/health", ctx -> ctx.json(new HealthResponse("UP", taskStore.size())));
        server.get("/metrics", ctx -> ctx.result("Metrics endpoint - TODO: implement Prometheus format"));

        // 异常处理
        server.exception(Exception.class, (e, ctx) -> {
            logger.error("Unhandled exception", e);
            ctx.status(500).json(new ErrorResponse(500, "Internal server error: " + e.getMessage()));
        });

        server.start(config.getServerConfig().port());
    }

    public void stop() {
        if (server != null) {
            server.stop();
        }
        if (scheduler != null) {
            scheduler.stop();
        }
        if (dispatcher != null) {
            dispatcher.stop();
        }
        AlertService.getInstance().stop();
    }

    public record HealthResponse(String status, long taskCount) {}
    public record ErrorResponse(int code, String message) {}
}
