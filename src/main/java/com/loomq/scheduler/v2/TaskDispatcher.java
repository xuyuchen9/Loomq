package com.loomq.scheduler.v2;

import com.loomq.entity.Task;

/**
 * 任务分发器接口
 * 由执行层实现，负责实际的 webhook 调用
 */
public interface TaskDispatcher {

    /**
     * 分发任务
     * 执行 webhook 调用、重试、状态更新等
     *
     * @param task 待执行的任务
     */
    void dispatch(Task task);
}
