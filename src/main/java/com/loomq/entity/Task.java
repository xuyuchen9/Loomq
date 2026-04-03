package com.loomq.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 任务实体
 */
public class Task {
    /**
     * 任务ID，全局唯一
     */
    private String taskId;

    /**
     * 业务幂等键，同一业务实体只出现一次
     */
    private String bizKey;

    /**
     * 请求级幂等键
     */
    private String idempotencyKey;

    /**
     * 任务状态
     */
    private volatile TaskStatus status;

    /**
     * 状态转换锁（用于 CAS 操作）
     */
    private final Object statusLock = new Object();

    /**
     * 触发时间戳（毫秒）
     */
    private long triggerTime;

    /**
     * 回调地址
     */
    private String webhookUrl;

    /**
     * HTTP 方法
     */
    private String method;

    /**
     * 请求头
     */
    private Map<String, String> headers;

    /**
     * 请求体（JSON字符串）
     */
    private String payload;

    /**
     * 最大重试次数
     */
    private int maxRetry;

    /**
     * 当前重试次数
     */
    private int retryCount;

    /**
     * 超时时间（毫秒）
     */
    private long timeoutMs;

    /**
     * 乐观锁版本号
     */
    private long version;

    /**
     * 创建时间
     */
    private long createTime;

    /**
     * 更新时间
     */
    private long updateTime;

    /**
     * 最后错误信息
     */
    private String lastError;

    /**
     * 唤醒时间 (运行时，不持久化)
     * 用于计算 webhook 延迟和总延迟
     */
    private transient long wakeTime;

    /**
     * 执行开始时间 (运行时，不持久化)
     */
    private transient long executionStartTime;

    public Task() {
        this.headers = new HashMap<>();
        this.method = "POST";
        this.maxRetry = 5;
        this.retryCount = 0;
        this.timeoutMs = 3000;
        this.version = 1;
        this.status = TaskStatus.PENDING;
    }

    // ========== Builder Pattern ==========

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Task task = new Task();

        public Builder taskId(String taskId) {
            task.taskId = taskId;
            return this;
        }

        public Builder bizKey(String bizKey) {
            task.bizKey = bizKey;
            return this;
        }

        public Builder idempotencyKey(String idempotencyKey) {
            task.idempotencyKey = idempotencyKey;
            return this;
        }

        public Builder status(TaskStatus status) {
            task.status = status;
            return this;
        }

        public Builder triggerTime(long triggerTime) {
            task.triggerTime = triggerTime;
            return this;
        }

        public Builder webhookUrl(String webhookUrl) {
            task.webhookUrl = webhookUrl;
            return this;
        }

        public Builder method(String method) {
            task.method = method;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            task.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
            return this;
        }

        public Builder header(String key, String value) {
            task.headers.put(key, value);
            return this;
        }

        public Builder payload(String payload) {
            task.payload = payload;
            return this;
        }

        public Builder maxRetry(int maxRetry) {
            task.maxRetry = maxRetry;
            return this;
        }

        public Builder retryCount(int retryCount) {
            task.retryCount = retryCount;
            return this;
        }

        public Builder timeoutMs(long timeoutMs) {
            task.timeoutMs = timeoutMs;
            return this;
        }

        public Builder version(long version) {
            task.version = version;
            return this;
        }

        public Builder createTime(long createTime) {
            task.createTime = createTime;
            return this;
        }

        public Builder updateTime(long updateTime) {
            task.updateTime = updateTime;
            return this;
        }

        public Builder lastError(String lastError) {
            task.lastError = lastError;
            return this;
        }

        public Task build() {
            Objects.requireNonNull(task.taskId, "taskId is required");
            Objects.requireNonNull(task.webhookUrl, "webhookUrl is required");
            if (task.createTime == 0) {
                task.createTime = System.currentTimeMillis();
            }
            if (task.updateTime == 0) {
                task.updateTime = task.createTime;
            }
            return task;
        }
    }

    // ========== Business Methods ==========

    /**
     * 增加重试次数
     */
    public void incrementRetry() {
        this.retryCount++;
        this.updateTime = System.currentTimeMillis();
    }

    /**
     * 是否还能重试
     */
    public boolean canRetry() {
        return retryCount < maxRetry && !status.isTerminal();
    }

    /**
     * 更新版本号
     */
    public void incrementVersion() {
        this.version++;
        this.updateTime = System.currentTimeMillis();
    }

    /**
     * 设置错误并更新状态
     */
    public void setError(String error) {
        this.lastError = error;
        this.updateTime = System.currentTimeMillis();
    }

    // ========== 原子状态转换方法 ==========

    /**
     * 尝试转换状态（CAS 操作）
     * 只有当前状态与期望状态匹配时才转换
     *
     * @param expectedStatus 期望的当前状态
     * @param newStatus 新状态
     * @return 是否转换成功
     */
    public boolean compareAndSetStatus(TaskStatus expectedStatus, TaskStatus newStatus) {
        synchronized (statusLock) {
            if (this.status == expectedStatus) {
                this.status = newStatus;
                this.updateTime = System.currentTimeMillis();
                return true;
            }
            return false;
        }
    }

    /**
     * 尝试取消任务（原子操作）
     * 只有在可取消状态下才能成功
     *
     * @return 是否取消成功
     */
    public boolean tryCancel() {
        synchronized (statusLock) {
            if (this.status.isCancellable()) {
                this.status = TaskStatus.CANCELLED;
                this.updateTime = System.currentTimeMillis();
                return true;
            }
            return false;
        }
    }

    /**
     * 尝试开始执行（原子操作）
     * 只有在 SCHEDULED 或 RETRY_WAIT 状态下才能开始执行
     *
     * @return 是否成功开始执行
     */
    public boolean tryStartExecution() {
        synchronized (statusLock) {
            if (this.status == TaskStatus.SCHEDULED ||
                this.status == TaskStatus.RETRY_WAIT) {
                this.status = TaskStatus.DISPATCHING;
                this.updateTime = System.currentTimeMillis();
                return true;
            }
            // 如果已经被取消或其他终态，返回 false
            return false;
        }
    }

    /**
     * 检查是否可以执行（非阻塞检查）
     */
    public boolean canExecute() {
        return !this.status.isTerminal();
    }

    /**
     * 转换到 ACKED 状态（原子操作）
     */
    public boolean transitionToAcked() {
        synchronized (statusLock) {
            if (this.status == TaskStatus.DISPATCHING) {
                this.status = TaskStatus.ACKED;
                this.updateTime = System.currentTimeMillis();
                return true;
            }
            return false;
        }
    }

    /**
     * 转换到 RETRY_WAIT 状态（原子操作）
     */
    public boolean transitionToRetry(int maxRetry) {
        synchronized (statusLock) {
            if (this.status == TaskStatus.DISPATCHING) {
                this.retryCount++;
                if (this.retryCount >= maxRetry) {
                    this.status = TaskStatus.FAILED_TERMINAL;
                    this.updateTime = System.currentTimeMillis();
                    return true;
                }
                this.status = TaskStatus.RETRY_WAIT;
                this.updateTime = System.currentTimeMillis();
                return true;
            }
            return false;
        }
    }

    /**
     * 转换到 FAILED_TERMINAL 状态（原子操作）
     */
    public boolean transitionToDead(String error) {
        synchronized (statusLock) {
            if (!this.status.isTerminal()) {
                this.status = TaskStatus.FAILED_TERMINAL;
                this.lastError = error;
                this.updateTime = System.currentTimeMillis();
                return true;
            }
            return false;
        }
    }

    // ========== Getters and Setters ==========

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getBizKey() {
        return bizKey;
    }

    public void setBizKey(String bizKey) {
        this.bizKey = bizKey;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
        this.updateTime = System.currentTimeMillis();
    }

    public long getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(long triggerTime) {
        this.triggerTime = triggerTime;
    }

    public String getWebhookUrl() {
        return webhookUrl;
    }

    public void setWebhookUrl(String webhookUrl) {
        this.webhookUrl = webhookUrl;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public long getWakeTime() {
        return wakeTime;
    }

    public void setWakeTime(long wakeTime) {
        this.wakeTime = wakeTime;
    }

    public long getExecutionStartTime() {
        return executionStartTime;
    }

    public void setExecutionStartTime(long executionStartTime) {
        this.executionStartTime = executionStartTime;
    }

    @Override
    public String toString() {
        return "Task{" +
                "taskId='" + taskId + '\'' +
                ", bizKey='" + bizKey + '\'' +
                ", status=" + status +
                ", triggerTime=" + triggerTime +
                ", webhookUrl='" + webhookUrl + '\'' +
                ", retryCount=" + retryCount +
                ", version=" + version +
                '}';
    }
}
