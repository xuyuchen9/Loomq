package com.loomq.entity;

import java.util.Objects;

/**
 * 任务事件记录
 * 所有状态变化都通过事件记录
 */
public class TaskEvent {
    /**
     * 事件ID
     */
    private String eventId;

    /**
     * 任务ID
     */
    private String taskId;

    /**
     * 事件类型
     */
    private EventType eventType;

    /**
     * 事件发生时的版本号
     */
    private long version;

    /**
     * 事件发生前版本号
     */
    private long prevVersion;

    /**
     * 事件时间戳
     */
    private long eventTime;

    /**
     * 事件详情（JSON字符串）
     */
    private String payload;

    public TaskEvent() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final TaskEvent event = new TaskEvent();

        public Builder eventId(String eventId) {
            event.eventId = eventId;
            return this;
        }

        public Builder taskId(String taskId) {
            event.taskId = taskId;
            return this;
        }

        public Builder eventType(EventType eventType) {
            event.eventType = eventType;
            return this;
        }

        public Builder version(long version) {
            event.version = version;
            return this;
        }

        public Builder prevVersion(long prevVersion) {
            event.prevVersion = prevVersion;
            return this;
        }

        public Builder eventTime(long eventTime) {
            event.eventTime = eventTime;
            return this;
        }

        public Builder payload(String payload) {
            event.payload = payload;
            return this;
        }

        public TaskEvent build() {
            Objects.requireNonNull(event.eventId, "eventId is required");
            Objects.requireNonNull(event.taskId, "taskId is required");
            Objects.requireNonNull(event.eventType, "eventType is required");
            if (event.eventTime == 0) {
                event.eventTime = System.currentTimeMillis();
            }
            return event;
        }
    }

    // ========== Getters and Setters ==========

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getPrevVersion() {
        return prevVersion;
    }

    public void setPrevVersion(long prevVersion) {
        this.prevVersion = prevVersion;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "TaskEvent{" +
                "eventId='" + eventId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", eventType=" + eventType +
                ", version=" + version +
                ", eventTime=" + eventTime +
                '}';
    }
}
