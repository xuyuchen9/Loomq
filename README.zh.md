# LoomQ

[![Java 21](https://img.shields.io/badge/Java-21-blue)](https://openjdk.org/projects/jdk/21/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

> 基于 Java 21 虚拟线程的高性能延迟任务队列。无需 Redis，无需 RabbitMQ，纯 Java 实现。

[English Documentation](README.md)

---

## ✨ 特性

- **零算法设计** - 利用 Java 21 虚拟线程的 `Thread.sleep()` 实现优雅的延迟调度
- **高性能** - 支持百万级延迟任务，内存占用极低
- **持久化存储** - WAL（预写日志）保证数据安全
- **精确一次投递** - 至少一次语义 + 幂等 Webhook 分发
- **RESTful API** - 简单的 HTTP 接口管理任务
- **可观测性** - 内置指标采集和健康检查
- **自动恢复** - 重启后自动从 WAL 日志恢复状态

## 🚀 快速开始

### 环境要求

- Java 21 或更高版本
- Maven 3.8+

### 构建

```bash
mvn clean package -DskipTests
```

### 运行

```bash
java --enable-preview -jar target/loomq-0.1.0-SNAPSHOT-shaded.jar
```

或者直接运行：

```bash
mvn exec:java -Dexec.mainClass="com.loomq.LoomqApplication"
```

## 📡 API 接口

### 创建任务

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "callbackUrl": "https://example.com/webhook",
    "delayMillis": 60000,
    "payload": {"orderId": "12345"}
  }'
```

响应：
```json
{
  "taskId": "task_abc123",
  "status": "PENDING"
}
```

### 查询任务

```bash
curl http://localhost:8080/api/v1/tasks/{taskId}
```

### 取消任务

```bash
curl -X DELETE http://localhost:8080/api/v1/tasks/{taskId}
```

### 修改任务

```bash
curl -X PATCH http://localhost:8080/api/v1/tasks/{taskId} \
  -H "Content-Type: application/json" \
  -d '{
    "delayMillis": 120000
  }'
```

### 立即触发

```bash
curl -X POST http://localhost:8080/api/v1/tasks/{taskId}/fire-now
```

### 健康检查

```bash
curl http://localhost:8080/health
```

### 指标数据

```bash
curl http://localhost:8080/metrics
```

## ⚙️ 配置

编辑 `src/main/resources/application.yml`：

```yaml
server:
  host: "0.0.0.0"
  port: 8080

wal:
  data_dir: "./data/wal"
  segment_size_mb: 64
  flush_strategy: "batch"  # per_record, batch, async

scheduler:
  max_pending_tasks: 1000000

dispatcher:
  http_timeout_ms: 3000
  max_concurrent_dispatches: 1000

retry:
  initial_delay_ms: 1000
  max_delay_ms: 60000
  multiplier: 2.0
  default_max_retry: 5
```

## 🏗️ 架构

```
┌─────────────────────────────────────────────────────────────┐
│                        HTTP API 层                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   创建任务   │  │   取消任务   │  │   查询/修改      │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        任务调度器                             │
│            (虚拟线程池 + 时间分桶调度)                          │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
       ┌──────────┐    ┌──────────┐    ┌──────────┐
       │   WAL    │    │   任务   │    │ Webhook  │
       │   引擎   │◄──►│   存储   │───►│  分发器   │
       │(持久化)  │    │(内存)    │    │ (重试)   │
       └──────────┘    └──────────┘    └──────────┘
```

### 核心组件

| 组件 | 说明 |
|------|------|
| **WAL 引擎** | 预写日志持久化，支持同步/异步刷盘 |
| **任务存储** | 内存任务存储，跳表索引 |
| **调度器** | 基于时间轮的虚拟线程调度 |
| **分发器** | Webhook 分发，指数退避重试 |
| **恢复服务** | 启动时从 WAL 自动恢复状态 |

## 🔥 性能

8核 CPU、16GB 内存环境下的基准测试结果：

| 指标 | 数值 |
|------|------|
| 任务创建 | ~50,000 TPS |
| 并发任务数 | 1,000,000+ |
| 分发延迟 | P99 < 10ms |
| 单任务内存 | ~200 字节 |

运行基准测试：

```bash
mvn test -Dtest=BenchmarkTest
```

## 🧪 测试

```bash
# 运行全部测试
mvn test

# 运行指定测试
mvn test -Dtest=IntegrationTest

# 生成覆盖率报告
mvn jacoco:report
```

## 📁 项目结构

```
loomq/
├── src/main/java/com/loomq/
│   ├── LoomqApplication.java      # 启动入口
│   ├── common/                     # 工具类和指标
│   ├── config/                     # 配置类
│   ├── dispatcher/                 # Webhook 分发器
│   ├── entity/                     # 领域模型
│   ├── gateway/                    # HTTP 控制器
│   ├── recovery/                   # WAL 恢复
│   ├── scheduler/                  # 任务调度器
│   ├── store/                      # 内存存储
│   └── wal/                        # WAL 引擎
├── src/main/resources/
│   └── application.yml             # 配置文件
└── src/test/                       # 测试代码
```

## 🤝 贡献

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing`)
3. 提交更改 (`git commit -m '添加新特性'`)
4. 推送分支 (`git push origin feature/amazing`)
5. 创建 Pull Request

## 📄 许可证

本项目采用 Apache License 2.0 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- Java 21 虚拟线程让百万级并发任务成为可能
- Javalin 提供轻量级 Web 框架
- Jackson 提供 JSON 处理能力
