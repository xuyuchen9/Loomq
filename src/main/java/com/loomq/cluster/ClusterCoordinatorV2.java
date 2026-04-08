package com.loomq.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 集群协调器 V2 - 分片稳定性收口 (v0.4.4)
 *
 * 改进点：
 * 1. 路由表版本控制 - CAS 语义更新，版本号单调递增
 * 2. 节点抖动判定 - 连续 N 次超时才标记失联，抖动期间禁止频繁切换路由
 * 3. 故障时策略 - 旧节点任务继续执行，新请求路由到新节点
 * 4. 幂等与路由一致性 - 分片键到 shard 的映射固定
 *
 * @author loomq
 * @since v0.4.4
 */
public class ClusterCoordinatorV2 {

    private static final Logger logger = LoggerFactory.getLogger(ClusterCoordinatorV2.class);

    // 默认心跳间隔（毫秒）
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 3000;

    // 默认心跳超时（毫秒）- 固化配置
    public static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 3000;

    // 默认抖动阈值（连续失败次数）
    public static final int DEFAULT_FLAPPING_THRESHOLD = 3;

    // 抖动窗口（毫秒）
    public static final long DEFAULT_FLAPPING_WINDOW_MS = 30000;

    // 集群配置
    private final ClusterConfig config;

    // 路由表（带版本控制）
    private final RoutingTable routingTable;

    // 本地节点
    private final LocalShardNode localNode;

    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 心跳执行器
    private ScheduledExecutorService heartbeatExecutor;

    // 心跳配置
    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;

    // 节点健康状态跟踪
    private final ConcurrentHashMap<String, NodeHealth> nodeHealthMap;

    // 状态变更监听器
    private final List<ClusterStateListener> stateListeners;

    // 路由表版本号
    private final AtomicLong routingTableVersion;

    // 故障时策略配置
    private final FailureHandlingConfig failureConfig;

    /**
     * 创建集群协调器 V2
     */
    public ClusterCoordinatorV2(ClusterConfig config, LocalShardNode localNode) {
        this(config, localNode, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS);
    }

    public ClusterCoordinatorV2(ClusterConfig config, LocalShardNode localNode,
                                 long heartbeatIntervalMs, long heartbeatTimeoutMs) {
        this.config = config;
        this.localNode = localNode;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;

        // 初始化路由表（带抖动检测）
        RoutingTable.RoutingTableConfig routingConfig = new RoutingTable.RoutingTableConfig(
                DEFAULT_FLAPPING_THRESHOLD,
                DEFAULT_FLAPPING_WINDOW_MS,
                5000
        );
        this.routingTable = new RoutingTable(routingConfig);

        this.nodeHealthMap = new ConcurrentHashMap<>();
        this.stateListeners = new CopyOnWriteArrayList<>();
        this.routingTableVersion = new AtomicLong(0);
        this.failureConfig = FailureHandlingConfig.defaultConfig();

        // 注册路由表监听器
        this.routingTable.addListener(this::onRoutingTableChanged);

        logger.info("ClusterCoordinatorV2 created for cluster: {}", config.getClusterName());
    }

    // ========== 生命周期管理 ==========

    /**
     * 启动协调器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        logger.info("Starting ClusterCoordinatorV2...");

        // 1. 初始化本地节点
        initializeLocalNode();

        // 2. 初始化路由表
        initializeRoutingTable();

        // 3. 启动心跳
        startHeartbeat();

        logger.info("ClusterCoordinatorV2 started, routing table version: {}",
                routingTable.getVersion());
    }

    /**
     * 停止协调器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping ClusterCoordinatorV2...");

        // 停止心跳
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
            try {
                heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        logger.info("ClusterCoordinatorV2 stopped");
    }

    // ========== 初始化方法 ==========

    private void initializeLocalNode() {
        // 确保本地节点已启动
        if (localNode.getState() == ShardNode.State.JOINING) {
            localNode.start();
        }

        NodeHealth health = new NodeHealth(
                localNode.getShardId(),
                NodeHealth.Status.HEALTHY,
                0,
                Instant.now(),
                Instant.now()
        );
        nodeHealthMap.put(localNode.getShardId(), health);

        logger.info("Local node initialized: {} with state {}",
                localNode.getShardId(), localNode.getState());
    }

    private void initializeRoutingTable() {
        ShardRouter router = new ShardRouter();

        // 添加所有配置节点到路由器
        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            int shardIndex = parseShardIndex(nodeConfig.shardId());
            LocalShardNode node = new LocalShardNode(
                    shardIndex,
                    config.getTotalShards(),
                    nodeConfig.host(),
                    nodeConfig.port(),
                    nodeConfig.weight()
            );
            router.addNode(node);
        }

        // 构建节点状态映射
        Map<String, ShardNode.State> nodeStates = new HashMap<>();
        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            nodeStates.put(nodeConfig.shardId(), ShardNode.State.ACTIVE);
        }

        // 强制初始化路由表
        routingTable.forceUpdate(router, nodeStates);
        routingTableVersion.set(routingTable.getVersion());

        logger.info("Routing table initialized with version {}, {} nodes",
                routingTable.getVersion(), config.getNodes().size());
    }

    // ========== 心跳与健康管理 ==========

    private void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-heartbeat-v2");
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(
                this::checkNodeHealth,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS
        );

        logger.info("Heartbeat started, interval={}ms, timeout={}ms",
                heartbeatIntervalMs, heartbeatTimeoutMs);
    }

    /**
     * 检查节点健康
     */
    private void checkNodeHealth() {
        if (!running.get()) {
            return;
        }

        // 更新本地节点心跳
        localNode.heartbeat();
        updateNodeHealth(localNode.getShardId(), NodeHealth.Status.HEALTHY);

        Instant now = Instant.now();

        // 检查其他节点
        for (Map.Entry<String, NodeHealth> entry : nodeHealthMap.entrySet()) {
            String shardId = entry.getKey();
            NodeHealth health = entry.getValue();

            // 跳过本地节点
            if (shardId.equals(localNode.getShardId())) {
                continue;
            }

            // 计算心跳超时
            long lastSeenMs = java.time.Duration.between(health.lastHeartbeat(), now).toMillis();

            if (lastSeenMs > heartbeatTimeoutMs) {
                // 记录失败
                int newConsecutiveFailures = health.consecutiveFailures() + 1;
                updateNodeHealth(shardId, NodeHealth.Status.SUSPECT, newConsecutiveFailures);

                logger.warn("Node {} heartbeat timeout (last seen {}ms ago), consecutive failures: {}",
                        shardId, lastSeenMs, newConsecutiveFailures);

                // 检查是否达到抖动阈值
                if (newConsecutiveFailures >= DEFAULT_FLAPPING_THRESHOLD) {
                    markNodeOffline(shardId);
                }
            }
        }
    }

    /**
     * 标记节点离线
     */
    private void markNodeOffline(String shardId) {
        NodeHealth currentHealth = nodeHealthMap.get(shardId);
        if (currentHealth == null || currentHealth.status() == NodeHealth.Status.OFFLINE) {
            return;
        }

        // 检查节点是否正在抖动
        if (routingTable.isNodeFlapping(shardId)) {
            logger.warn("Node {} is flapping, delaying offline marking", shardId);
            return;
        }

        updateNodeHealth(shardId, NodeHealth.Status.OFFLINE, currentHealth.consecutiveFailures());

        // 更新路由表（CAS 操作）
        updateRoutingTableForOfflineNode(shardId);

        // 通知监听器
        notifyNodeOffline(shardId);

        logger.info("Node {} marked as OFFLINE, routing table version: {}",
                shardId, routingTable.getVersion());
    }

    /**
     * 更新节点健康状态
     */
    private void updateNodeHealth(String shardId, NodeHealth.Status status) {
        updateNodeHealth(shardId, status,
                status == NodeHealth.Status.HEALTHY ? 0 :
                        nodeHealthMap.getOrDefault(shardId,
                                new NodeHealth(shardId, status, 0, Instant.now(), Instant.now()))
                                .consecutiveFailures());
    }

    private void updateNodeHealth(String shardId, NodeHealth.Status status, int consecutiveFailures) {
        NodeHealth newHealth = new NodeHealth(
                shardId,
                status,
                consecutiveFailures,
                nodeHealthMap.getOrDefault(shardId,
                        new NodeHealth(shardId, status, 0, Instant.now(), Instant.now())).firstFailureTime(),
                status == NodeHealth.Status.HEALTHY ? Instant.now() :
                        nodeHealthMap.getOrDefault(shardId,
                                new NodeHealth(shardId, status, 0, Instant.now(), Instant.now())).lastHeartbeat()
        );

        nodeHealthMap.put(shardId, newHealth);
    }

    // ========== 路由表更新 ==========

    /**
     * 更新路由表以反映节点离线
     */
    private void updateRoutingTableForOfflineNode(String offlineShardId) {
        long currentVersion = routingTableVersion.get();

        // 获取当前路由表快照
        RoutingTable.TableSnapshot snapshot = routingTable.getSnapshot();
        if (snapshot.router() == null) {
            logger.warn("No router available for updating");
            return;
        }

        // 创建新的路由器（排除离线节点）
        ShardRouter newRouter = new ShardRouter();
        Map<String, ShardNode.State> newNodeStates = new HashMap<>();

        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            String shardId = nodeConfig.shardId();
            NodeHealth health = nodeHealthMap.get(shardId);

            ShardNode.State state;
            if (shardId.equals(offlineShardId)) {
                state = ShardNode.State.OFFLINE;
            } else if (health != null && health.status() == NodeHealth.Status.HEALTHY) {
                state = ShardNode.State.ACTIVE;
            } else {
                state = ShardNode.State.JOINING;
            }

            newNodeStates.put(shardId, state);

            // 只添加活跃节点到路由器
            if (state == ShardNode.State.ACTIVE) {
                int shardIndex = parseShardIndex(shardId);
                LocalShardNode node = new LocalShardNode(
                        shardIndex,
                        config.getTotalShards(),
                        nodeConfig.host(),
                        nodeConfig.port(),
                        nodeConfig.weight()
                );
                newRouter.addNode(node);
            }
        }

        // CAS 更新路由表
        boolean updated = routingTable.compareAndSwap(currentVersion, newRouter, newNodeStates);
        if (updated) {
            routingTableVersion.incrementAndGet();
            logger.info("Routing table updated for offline node: {}, new version: {}",
                    offlineShardId, routingTable.getVersion());
        } else {
            logger.warn("Failed to update routing table for offline node: {}", offlineShardId);
        }
    }

    /**
     * 路由表变更回调
     */
    private void onRoutingTableChanged(RoutingTable.TableSnapshot snapshot) {
        routingTableVersion.set(snapshot.version());
        logger.debug("Routing table changed: version={}, activeNodes={}",
                snapshot.version(), snapshot.getActiveNodeCount());
    }

    // ========== 公共 API ==========

    /**
     * 接收心跳（用于远程节点）
     */
    public void receiveHeartbeat(String shardId) {
        NodeHealth currentHealth = nodeHealthMap.get(shardId);

        if (currentHealth != null && currentHealth.status() == NodeHealth.Status.OFFLINE) {
            // 节点从离线恢复
            logger.info("Node {} recovered, marking as HEALTHY", shardId);
            updateNodeHealth(shardId, NodeHealth.Status.HEALTHY, 0);

            // 恢复节点到路由表
            recoverNodeToRoutingTable(shardId);

            notifyNodeRecovered(shardId);
        } else {
            updateNodeHealth(shardId, NodeHealth.Status.HEALTHY, 0);
        }
    }

    /**
     * 恢复节点到路由表
     */
    private void recoverNodeToRoutingTable(String recoveredShardId) {
        long currentVersion = routingTableVersion.get();

        RoutingTable.TableSnapshot snapshot = routingTable.getSnapshot();
        if (snapshot.router() == null) {
            return;
        }

        // 创建新的路由器（包含恢复的节点）
        ShardRouter newRouter = new ShardRouter();
        Map<String, ShardNode.State> newNodeStates = new HashMap<>(snapshot.nodeStates());
        newNodeStates.put(recoveredShardId, ShardNode.State.ACTIVE);

        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            String shardId = nodeConfig.shardId();
            ShardNode.State state = newNodeStates.getOrDefault(shardId, ShardNode.State.JOINING);

            if (state == ShardNode.State.ACTIVE) {
                int shardIndex = parseShardIndex(shardId);
                LocalShardNode node = new LocalShardNode(
                        shardIndex,
                        config.getTotalShards(),
                        nodeConfig.host(),
                        nodeConfig.port(),
                        nodeConfig.weight()
                );
                newRouter.addNode(node);
            }
        }

        // CAS 更新路由表
        boolean updated = routingTable.compareAndSwap(currentVersion, newRouter, newNodeStates);
        if (updated) {
            routingTableVersion.incrementAndGet();
            logger.info("Routing table updated for recovered node: {}, new version: {}",
                    recoveredShardId, routingTable.getVersion());
        }
    }

    /**
     * 路由任务到分片节点（带版本检查）
     */
    public RoutingTable.RoutingResult route(String taskId, Optional<Long> expectedVersion) {
        return routingTable.route(taskId, expectedVersion);
    }

    /**
     * 简单路由（不带版本检查）
     */
    public Optional<ShardNode> route(String taskId) {
        return routingTable.routeSimple(taskId);
    }

    /**
     * 检查任务是否应由本地节点处理
     */
    public boolean isLocalTask(String taskId) {
        Optional<ShardNode> node = route(taskId);
        return node.isPresent() &&
                node.get().getShardId().equals(localNode.getShardId());
    }

    /**
     * 获取当前路由表版本
     */
    public long getRoutingTableVersion() {
        return routingTable.getVersion();
    }

    /**
     * 获取路由表
     */
    public RoutingTable getRoutingTable() {
        return routingTable;
    }

    /**
     * 获取节点健康状态
     */
    public Optional<NodeHealth> getNodeHealth(String shardId) {
        return Optional.ofNullable(nodeHealthMap.get(shardId));
    }

    /**
     * 获取所有节点健康状态
     */
    public Map<String, NodeHealth> getAllNodeHealth() {
        return Map.copyOf(nodeHealthMap);
    }

    /**
     * 获取集群状态
     */
    public ClusterState getClusterState() {
        RoutingTable.TableSnapshot snapshot = routingTable.getSnapshot();

        return new ClusterState(
                config.getClusterName(),
                config.getTotalShards(),
                (int) snapshot.getActiveNodeCount(),
                (int) snapshot.getOfflineNodeCount(),
                routingTable.getVersion(),
                running.get()
        );
    }

    // ========== 监听器管理 ==========

    public void addStateListener(ClusterStateListener listener) {
        stateListeners.add(listener);
    }

    public void removeStateListener(ClusterStateListener listener) {
        stateListeners.remove(listener);
    }

    private void notifyNodeOffline(String shardId) {
        for (ClusterStateListener listener : stateListeners) {
            try {
                listener.onNodeOffline(shardId);
            } catch (Exception e) {
                logger.error("State listener error", e);
            }
        }
    }

    private void notifyNodeRecovered(String shardId) {
        for (ClusterStateListener listener : stateListeners) {
            try {
                listener.onNodeRecovered(shardId);
            } catch (Exception e) {
                logger.error("State listener error", e);
            }
        }
    }

    // ========== 工具方法 ==========

    private int parseShardIndex(String shardId) {
        if (shardId.startsWith("shard-")) {
            try {
                return Integer.parseInt(shardId.substring(6));
            } catch (NumberFormatException e) {
                logger.warn("Invalid shardId format: {}", shardId);
            }
        }
        return -1;
    }

    // ========== 记录定义 ==========

    /**
     * 节点健康状态
     */
    public record NodeHealth(
            String shardId,
            Status status,
            int consecutiveFailures,
            Instant firstFailureTime,
            Instant lastHeartbeat
    ) {
        public enum Status {
            HEALTHY,    // 健康
            SUSPECT,    // 可疑（部分心跳超时）
            OFFLINE     // 离线
        }

        public boolean isHealthy() {
            return status == Status.HEALTHY;
        }

        public boolean isOffline() {
            return status == Status.OFFLINE;
        }
    }

    /**
     * 集群状态
     */
    public record ClusterState(
            String clusterName,
            int totalShards,
            int activeNodes,
            int offlineNodes,
            long routingTableVersion,
            boolean coordinatorRunning
    ) {
        public boolean isHealthy() {
            return activeNodes == totalShards && offlineNodes == 0;
        }

        @Override
        public String toString() {
            return String.format("ClusterState{name=%s, shards=%d, active=%d, offline=%d, version=%d}",
                    clusterName, totalShards, activeNodes, offlineNodes, routingTableVersion);
        }
    }

    /**
     * 故障处理配置
     */
    public record FailureHandlingConfig(
            boolean keepRunningTasks,      // 故障时是否继续执行旧节点任务
            boolean rerouteNewRequests,    // 是否将新请求路由到新节点
            long taskDrainTimeoutMs        // 任务排空超时
    ) {
        public static FailureHandlingConfig defaultConfig() {
            return new FailureHandlingConfig(
                    true,    // 保持旧节点任务运行
                    true,    // 新请求路由到新节点
                    300000   // 5 分钟排空超时
            );
        }
    }

    /**
     * 集群状态监听器
     */
    public interface ClusterStateListener {
        default void onNodeOffline(String shardId) {}
        default void onNodeRecovered(String shardId) {}
        default void onRoutingTableChanged(long newVersion) {}
    }
}
