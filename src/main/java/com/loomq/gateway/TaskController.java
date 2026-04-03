package com.loomq.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loomq.common.ErrorCode;
import com.loomq.common.IdGenerator;
import com.loomq.common.MetricsCollector;
import com.loomq.common.Result;
import com.loomq.dispatcher.WebhookDispatcher;
import com.loomq.entity.EventType;
import com.loomq.entity.Task;
import com.loomq.entity.TaskEvent;
import com.loomq.entity.TaskStatus;
import com.loomq.scheduler.TaskScheduler;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalEngine;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * 任务 API 控制器
 */
public class TaskController {
    private static final Logger logger = LoggerFactory.getLogger(TaskController.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final WalEngine walEngine;
    private final TaskStore taskStore;
    private final TaskScheduler scheduler;
    private final WebhookDispatcher dispatcher;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TaskController(WalEngine walEngine, TaskStore taskStore,
                          TaskScheduler scheduler, WebhookDispatcher dispatcher) {
        this.walEngine = walEngine;
        this.taskStore = taskStore;
        this.scheduler = scheduler;
        this.dispatcher = dispatcher;
    }

    /**
     * 创建任务
     * POST /api/v1/tasks
     */
    public void createTask(Context ctx) {
        try {
            CreateTaskRequest request = ctx.bodyAsClass(CreateTaskRequest.class);

            // 参数校验
            if (request.webhookUrl() == null || request.webhookUrl().isEmpty()) {
                ctx.json(Result.error(ErrorCode.INVALID_PARAM, "webhookUrl is required"));
                return;
            }

            // 计算触发时间
            long triggerTime;
            if (request.triggerTime() != null && request.triggerTime() > 0) {
                triggerTime = request.triggerTime();
            } else if (request.delayMs() != null && request.delayMs() > 0) {
                triggerTime = System.currentTimeMillis() + request.delayMs();
            } else {
                ctx.json(Result.error(ErrorCode.INVALID_PARAM, "triggerTime or delayMs is required"));
                return;
            }

            // 幂等检查
            String idempotencyKey = request.idempotencyKey();
            if (idempotencyKey != null && taskStore.existsByIdempotencyKey(idempotencyKey)) {
                Task existing = taskStore.getByIdempotencyKey(idempotencyKey);
                ctx.json(Result.success(new CreateTaskResponse(existing.getTaskId(), existing.getStatus().name())));
                return;
            }

            // 检查 bizKey 幂等
            String bizKey = request.bizKey();
            if (bizKey != null && taskStore.existsByBizKey(bizKey)) {
                Task existing = taskStore.getByBizKey(bizKey);
                ctx.json(Result.success(new CreateTaskResponse(existing.getTaskId(), existing.getStatus().name())));
                return;
            }

            // 生成任务ID
            String taskId = IdGenerator.generateTaskId();

            // 创建任务对象
            Task task = Task.builder()
                    .taskId(taskId)
                    .bizKey(bizKey)
                    .idempotencyKey(idempotencyKey)
                    .status(TaskStatus.PENDING)
                    .triggerTime(triggerTime)
                    .webhookUrl(request.webhookUrl())
                    .method(request.method() != null ? request.method() : "POST")
                    .headers(request.headers())
                    .payload(request.payload() != null ? toJson(request.payload()) : null)
                    .maxRetry(request.maxRetry() != null ? request.maxRetry() : 5)
                    .timeoutMs(request.timeoutMs() != null ? request.timeoutMs() : 3000)
                    .createTime(System.currentTimeMillis())
                    .build();

            // 写入 CREATE 事件
            TaskEvent createEvent = TaskEvent.builder()
                    .eventId(IdGenerator.generateEventId())
                    .taskId(taskId)
                    .eventType(EventType.CREATE)
                    .version(1)
                    .prevVersion(0)
                    .eventTime(System.currentTimeMillis())
                    .payload(toJson(task))
                    .build();
            walEngine.appendEvent(createEvent);

            // 存储任务
            taskStore.add(task);

            // 调度任务
            scheduler.schedule(task);

            // 记录审计日志
            auditLogger.info("TASK_CREATE|{}|{}|{}|{}", taskId, bizKey, request.webhookUrl(), triggerTime);

            // 记录指标
            MetricsCollector.getInstance().incrementTasksCreated();

            ctx.status(201).json(Result.success(new CreateTaskResponse(taskId, task.getStatus().name())));

        } catch (Exception e) {
            logger.error("Failed to create task", e);
            ctx.json(Result.error(ErrorCode.INTERNAL_ERROR, e.getMessage()));
        }
    }

    /**
     * 查询任务
     * GET /api/v1/tasks/{taskId}
     */
    public void getTask(Context ctx) {
        String taskId = ctx.pathParam("taskId");

        Task task = taskStore.get(taskId);
        if (task == null) {
            ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND));
            return;
        }

        ctx.json(Result.success(toTaskResponse(task)));
    }

    /**
     * 取消任务
     * DELETE /api/v1/tasks/{taskId}
     *
     * 语义说明：
     * Loomq 提供"最终一致取消语义"，而非"强一致取消"。
     * - 成功返回：任务状态已设为 CANCELLED，不会再被调度
     * - 但如果任务已在执行中，可能仍会完成 webhook 调用
     * - 下游服务必须实现幂等以处理可能的重复调用
     */
    public void cancelTask(Context ctx) {
        String taskId = ctx.pathParam("taskId");

        Task task = taskStore.get(taskId);
        if (task == null) {
            ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND));
            return;
        }

        // 原子操作：尝试取消
        // 如果任务已在执行中（DISPATCHING），此操作会失败
        if (!task.tryCancel()) {
            ctx.json(Result.error(ErrorCode.TASK_CANNOT_CANCEL,
                    "Task status is " + task.getStatus() + " and cannot be cancelled. " +
                    "Note: Task may already be executing."));
            return;
        }

        try {
            // 状态已由 tryCancel 设置为 CANCELLED
            task.incrementVersion();
            taskStore.update(task);

            // 从调度器移除（中断休眠线程）
            scheduler.unschedule(taskId);

            // 写入 CANCEL 事件
            TaskEvent cancelEvent = TaskEvent.builder()
                    .eventId(IdGenerator.generateEventId())
                    .taskId(taskId)
                    .eventType(EventType.CANCEL)
                    .version(task.getVersion())
                    .prevVersion(task.getVersion() - 1)
                    .eventTime(System.currentTimeMillis())
                    .build();
            walEngine.appendEvent(cancelEvent);

            // 记录审计日志
            auditLogger.info("TASK_CANCEL|{}|{}", taskId, task.getBizKey());

            // 记录指标
            MetricsCollector.getInstance().incrementTasksCancelled();

            ctx.json(Result.success(toTaskResponse(task)));

        } catch (IOException e) {
            logger.error("Failed to cancel task: {}", taskId, e);
            ctx.json(Result.error(ErrorCode.INTERNAL_ERROR, e.getMessage()));
        }
    }

    /**
     * 修改任务
     * PATCH /api/v1/tasks/{taskId}
     */
    public void modifyTask(Context ctx) {
        String taskId = ctx.pathParam("taskId");

        Task task = taskStore.get(taskId);
        if (task == null) {
            ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND));
            return;
        }

        if (!task.getStatus().isModifiable()) {
            ctx.json(Result.error(ErrorCode.TASK_CANNOT_MODIFY,
                    "Task status is " + task.getStatus() + " and cannot be modified"));
            return;
        }

        try {
            ModifyTaskRequest request = ctx.bodyAsClass(ModifyTaskRequest.class);

            long oldTriggerTime = task.getTriggerTime();

            // 版本校验
            if (request.version() != null && request.version() != task.getVersion()) {
                ctx.json(Result.error(ErrorCode.VERSION_CONFLICT,
                        "Version conflict: expected " + task.getVersion() + ", got " + request.version()));
                return;
            }

            // 应用修改
            if (request.triggerTime() != null) {
                task.setTriggerTime(request.triggerTime());
            }
            if (request.webhookUrl() != null) {
                task.setWebhookUrl(request.webhookUrl());
            }
            if (request.payload() != null) {
                task.setPayload(toJson(request.payload()));
            }
            if (request.maxRetry() != null) {
                task.setMaxRetry(request.maxRetry());
            }
            if (request.timeoutMs() != null) {
                task.setTimeoutMs(request.timeoutMs());
            }

            task.incrementVersion();
            taskStore.update(task);

            // 如果触发时间变化，重新调度
            if (oldTriggerTime != task.getTriggerTime()) {
                scheduler.reschedule(task);
            }

            // 写入 MODIFY 事件
            TaskEvent modifyEvent = TaskEvent.builder()
                    .eventId(IdGenerator.generateEventId())
                    .taskId(taskId)
                    .eventType(EventType.MODIFY)
                    .version(task.getVersion())
                    .prevVersion(task.getVersion() - 1)
                    .eventTime(System.currentTimeMillis())
                    .payload(toJson(request))
                    .build();
            walEngine.appendEvent(modifyEvent);

            // 记录审计日志
            auditLogger.info("TASK_MODIFY|{}|{}", taskId, task.getBizKey());

            ctx.json(Result.success(toTaskResponse(task)));

        } catch (Exception e) {
            logger.error("Failed to modify task: {}", taskId, e);
            ctx.json(Result.error(ErrorCode.INTERNAL_ERROR, e.getMessage()));
        }
    }

    /**
     * 立即触发
     * POST /api/v1/tasks/{taskId}/fire-now
     */
    public void fireNow(Context ctx) {
        String taskId = ctx.pathParam("taskId");

        Task task = taskStore.get(taskId);
        if (task == null) {
            ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND));
            return;
        }

        if (task.getStatus().isTerminal()) {
            ctx.json(Result.error(ErrorCode.TASK_ALREADY_TERMINATED,
                    "Task is already in terminal state: " + task.getStatus()));
            return;
        }

        try {
            // 更新触发时间为立即
            long newTriggerTime = System.currentTimeMillis();
            task.setTriggerTime(newTriggerTime);
            task.incrementVersion();
            taskStore.update(task);

            // 写入 FIRE_NOW 事件
            TaskEvent fireEvent = TaskEvent.builder()
                    .eventId(IdGenerator.generateEventId())
                    .taskId(taskId)
                    .eventType(EventType.FIRE_NOW)
                    .version(task.getVersion())
                    .prevVersion(task.getVersion() - 1)
                    .eventTime(newTriggerTime)
                    .build();
            walEngine.appendEvent(fireEvent);

            // 从调度器移除旧的调度
            scheduler.unschedule(taskId);

            // 记录审计日志
            auditLogger.info("TASK_FIRE_NOW|{}|{}", taskId, task.getBizKey());

            // 直接调用 dispatcher 执行，不依赖时间轮
            dispatcher.dispatch(task);

            ctx.json(Result.success(toTaskResponse(task)));

        } catch (IOException e) {
            logger.error("Failed to fire task: {}", taskId, e);
            ctx.json(Result.error(ErrorCode.INTERNAL_ERROR, e.getMessage()));
        }
    }

    // ========== Helper Methods ==========

    private TaskResponse toTaskResponse(Task task) {
        return new TaskResponse(
                task.getTaskId(),
                task.getBizKey(),
                task.getStatus().name(),
                task.getTriggerTime(),
                task.getWebhookUrl(),
                task.getMethod(),
                task.getHeaders(),
                task.getPayload(),
                task.getMaxRetry(),
                task.getRetryCount(),
                task.getTimeoutMs(),
                task.getVersion(),
                task.getCreateTime(),
                task.getUpdateTime(),
                task.getLastError()
        );
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return obj != null ? obj.toString() : null;
        }
    }

    // ========== DTOs ==========

    public record CreateTaskRequest(
            String bizKey,
            String idempotencyKey,
            Long delayMs,
            Long triggerTime,
            String webhookUrl,
            String method,
            Map<String, String> headers,
            Object payload,
            Integer maxRetry,
            Long timeoutMs
    ) {}

    public record ModifyTaskRequest(
            Long version,
            Long triggerTime,
            String webhookUrl,
            Object payload,
            Integer maxRetry,
            Long timeoutMs
    ) {}

    public record CreateTaskResponse(String taskId, String status) {}

    public record TaskResponse(
            String taskId,
            String bizKey,
            String status,
            long triggerTime,
            String webhookUrl,
            String method,
            Map<String, String> headers,
            String payload,
            int maxRetry,
            int retryCount,
            long timeoutMs,
            long version,
            long createTime,
            long updateTime,
            String lastError
    ) {}
}
