package com.loomq.v2;

import com.loomq.config.WalConfig;

/**
 * Loomq V2 配置
 *
 * 调度 SLA 说明：
 * - 正常模式：P95 ≤ bucketGranularityMs + dispatchLatencyMs
 * - 示例：bucket=1000ms 时，P95 ≈ 1000ms + 20ms = 1020ms
 * - 所有任务都有 0 ~ bucketGranularityMs 的调度误差
 *
 * SLA 生效条件（重要）：
 * - 系统未触发背压（dispatchQueue未满、Semaphore可用）
 * - 触发背压后进入"降级模式"，延迟不可预测
 * - 降级模式下系统优先保证"不崩溃"而非"不延迟"
 *
 * ACK 语义说明：
 * - ASYNC：publish 成功即返回，RPO < 100ms
 * - DURABLE：fsync 成功后返回，RPO = 0
 *
 * ACK 边界声明（重要）：
 * - ACK 只保证"持久化成功"，不保证"执行完成"
 * - 执行阶段可能因 webhook 失败、下游不可用等原因失败
 * - 任务最终状态以 WAL 记录的最终事件为准
 */
public record LoomqConfigV2(
        // 调度配置
        int bucketGranularityMs,      // 时间桶粒度（毫秒），决定调度精度
        int readyQueueCapacity,       // 就绪队列容量
        int dispatchRatePerSecond,    // 每秒最大分发速率（0=不限速）

        // 执行配置
        int maxConcurrency,           // 最大并发数
        int dispatchQueueCapacity,    // 分发队列容量

        // 重试配置
        int maxRetry,                 // 最大重试次数
        long retryBackoffMs,          // 重试退避基数（毫秒）

        // Webhook 配置
        int webhookConnectTimeoutMs,
        int webhookReadTimeoutMs,

        // WAL 配置
        String walDataDir,
        String walFlushStrategy,
        AckLevel defaultAckLevel      // 默认 ACK 级别
) {
    /**
     * ACK 级别
     */
    public enum AckLevel {
        ASYNC,    // publish 后返回，RPO < 100ms
        DURABLE   // fsync 后返回，RPO = 0
    }

    /**
     * 默认配置
     */
    public static LoomqConfigV2 defaults() {
        return new LoomqConfigV2(
                1000,      // 1秒桶粒度，P95 ≈ 1s + 20ms
                10000,     // 就绪队列 1万
                0,         // 不限速
                1000,      // 最大并发 1000
                50000,     // 分发队列 5万
                5,         // 最大重试 5 次
                1000,      // 重试退避 1秒
                5000,      // webhook 连接超时 5秒
                30000,     // webhook 读取超时 30秒
                "data/wal",
                "batch",
                AckLevel.ASYNC  // 默认异步
        );
    }

    /**
     * 获取调度 SLA 描述
     */
    public String getSlaDescription() {
        return String.format("P95 ≤ %dms (bucket) + ~20ms (dispatch)", bucketGranularityMs);
    }

    /**
     * 转换为 WalConfig
     */
    public WalConfig toWalConfig() {
        return new WalConfig() {
            @Override
            public String dataDir() {
                return walDataDir;
            }

            @Override
            public String flushStrategy() {
                return walFlushStrategy;
            }

            @Override
            public long batchFlushIntervalMs() {
                return 100;
            }

            @Override
            public int segmentSizeMb() {
                return 128;
            }

            @Override
            public boolean syncOnWrite() {
                return true;
            }
        };
    }
}
