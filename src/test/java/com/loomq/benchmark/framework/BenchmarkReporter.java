package com.loomq.benchmark.framework;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 压测报告生成器 (v0.4.3)
 *
 * 支持格式：Markdown, CSV, JSON
 *
 * @author loomq
 * @since v0.4.3
 */
public class BenchmarkReporter {

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(java.time.ZoneId.systemDefault());

    /**
     * 生成 Markdown 格式报告
     */
    public String generateMarkdown(BenchmarkBase.BenchmarkReport report) {
        StringBuilder sb = new StringBuilder();

        // 标题
        sb.append("# ").append(report.benchmarkName()).append("\n\n");

        // 元信息
        sb.append("## 元信息\n\n");
        sb.append("| 属性 | 值 |\n");
        sb.append("|------|-----|\n");
        sb.append("| 测试时间 | ").append(DATE_FORMATTER.format(report.timestamp())).append(" |\n");
        sb.append("| 重复次数 | ").append(report.config().repeatCount()).append(" |\n");
        sb.append("| 预热时长 | ").append(report.config().warmupDuration()).append(" |\n");
        sb.append("| 记录延迟 | ").append(report.config().recordLatencies() ? "是" : "否").append(" |\n");
        sb.append("\n");

        // 环境信息
        BenchmarkBase.BenchmarkEnvironment env = report.environment();
        sb.append("## 测试环境\n\n");
        sb.append("| 属性 | 值 |\n");
        sb.append("|------|-----|\n");
        sb.append("| 操作系统 | ").append(env.osName()).append(" ").append(env.osVersion()).append(" |\n");
        sb.append("| JDK版本 | ").append(env.javaVersion()).append(" |\n");
        sb.append("| JVM | ").append(env.jvmName()).append(" |\n");
        sb.append("| JVM参数 | ").append(env.jvmArgs()).append(" |\n");
        sb.append("| 处理器 | ").append(env.availableProcessors()).append(" 核 |\n");
        sb.append("| 最大内存 | ").append(env.maxMemoryMB()).append(" MB |\n");
        sb.append("| GC收集器 | ").append(env.gcCollectors()).append(" |\n");
        sb.append("\n");

        // 统计摘要
        BenchmarkBase.BenchmarkStatistics stats = report.statistics();
        sb.append("## 统计摘要\n\n");
        sb.append("### 吞吐量\n\n");
        sb.append("| 指标 | 值 |\n");
        sb.append("|------|-----|\n");
        sb.append("| 均值 | ").append(String.format("%.2f", stats.meanThroughput())).append(" ops/s |\n");
        sb.append("| 标准差 | ").append(String.format("%.2f", stats.stdThroughput())).append(" ops/s |\n");
        sb.append("| 变异系数(CV) | ").append(String.format("%.2f%%", stats.cvThroughput())).append(" |\n");
        sb.append("| 最小值 | ").append(String.format("%.2f", stats.minThroughput())).append(" ops/s |\n");
        sb.append("| 最大值 | ").append(String.format("%.2f", stats.maxThroughput())).append(" ops/s |\n");
        sb.append("\n");

        sb.append("### 延迟分位数\n\n");
        sb.append("| 分位 | 延迟(ms) |\n");
        sb.append("|------|----------|\n");
        sb.append("| P50 | ").append(stats.p50LatencyMs()).append(" |\n");
        sb.append("| P95 | ").append(stats.p95LatencyMs()).append(" |\n");
        sb.append("| P99 | ").append(stats.p99LatencyMs()).append(" |\n");
        sb.append("| P99.9 | ").append(stats.p999LatencyMs()).append(" |\n");
        sb.append("| Max | ").append(stats.maxLatencyMs()).append(" |\n");
        sb.append("\n");

        // 可信度评估
        sb.append("## 可信度评估\n\n");
        boolean isCredible = stats.cvThroughput() < 10.0 && stats.outlierCount() == 0;
        sb.append("**结论: ").append(isCredible ? "✅ 可信" : "⚠️ 存疑").append("**\n\n");
        sb.append("| 检查项 | 标准 | 实际值 | 结果 |\n");
        sb.append("|--------|------|--------|------|\n");
        sb.append("| 变异系数(CV) | < 10% | ")
                .append(String.format("%.2f%%", stats.cvThroughput())).append(" | ")
                .append(stats.cvThroughput() < 10.0 ? "✅ 通过" : "❌ 未通过").append(" |\n");
        sb.append("| 异常值数量 | = 0 | ").append(stats.outlierCount()).append(" | ")
                .append(stats.outlierCount() == 0 ? "✅ 通过" : "❌ 未通过").append(" |\n");
        sb.append("\n");

        if (!isCredible) {
            sb.append("> ⚠️ **注意**: 本次测试结果可信度较低。建议：\n");
            sb.append("> 1. 增加重复次数（建议 >= 10 次）\n");
            sb.append("> 2. 检查系统负载，关闭其他干扰程序\n");
            sb.append("> 3. 检查是否有频繁的 GC 暂停\n\n");
        }

        // 原始运行数据
        sb.append("## 原始运行数据\n\n");
        sb.append("| 运行# | 操作数 | 耗时(ms) | 吞吐量(ops/s) | 内存(MB) |\n");
        sb.append("|-------|--------|----------|---------------|----------|\n");

        int runNum = 1;
        for (BenchmarkBase.BenchmarkRunResult run : report.runs()) {
            sb.append("| ").append(runNum++)
                    .append(" | ").append(run.operationCount())
                    .append(" | ").append(run.elapsedMs())
                    .append(" | ").append(String.format("%.2f", run.throughput()))
                    .append(" | ").append(run.memoryUsedMB())
                    .append(" |\n");
        }
        sb.append("\n");

        // GC 统计
        sb.append("## GC 统计\n\n");
        boolean hasGcData = report.runs().stream()
                .anyMatch(r -> !r.gcStats().isEmpty());

        if (hasGcData) {
            sb.append("| 运行# | 收集器 | 次数 | 总时间(ms) | 平均暂停(ms) |\n");
            sb.append("|-------|--------|------|------------|--------------|\n");

            runNum = 1;
            for (BenchmarkBase.BenchmarkRunResult run : report.runs()) {
                for (BenchmarkBase.GcStats gc : run.gcStats().values()) {
                    sb.append("| ").append(runNum)
                            .append(" | ").append(gc.name())
                            .append(" | ").append(gc.collectionCount())
                            .append(" | ").append(gc.collectionTimeMs())
                            .append(" | ").append(String.format("%.3f", gc.avgPauseTimeMs()))
                            .append(" |\n");
                }
                runNum++;
            }
        } else {
            sb.append("未收集 GC 数据\n");
        }
        sb.append("\n");

        // 测试局限
        sb.append("## 测试局限\n\n");
        sb.append("1. **硬件依赖**: 测试结果高度依赖运行硬件配置\n");
        sb.append("2. **JVM 版本**: 不同 JDK 版本的性能特征可能有显著差异\n");
        sb.append("3. **系统负载**: 未控制系统负载，可能受后台进程影响\n");
        sb.append("4. **内存状态**: 未控制 JVM 堆内存初始状态\n");
        sb.append("5. **样本大小**: 单次测试的样本量可能不足以代表真实性能\n");
        sb.append("\n");

        // 结论
        sb.append("## 结论\n\n");
        sb.append("在测试环境下，**").append(report.benchmarkName()).append("** 的：\n\n");
        sb.append("- **平均吞吐量**: ").append(String.format("%.2f", stats.meanThroughput())).append(" ops/s\n");
        sb.append("- **P95 延迟**: ").append(stats.p95LatencyMs()).append(" ms\n");
        sb.append("- **可信度**: ").append(isCredible ? "高 (CV < 10%)" : "低 (CV >= 10%)").append("\n\n");

        return sb.toString();
    }

    /**
     * 生成 CSV 格式报告
     */
    public String generateCsv(BenchmarkBase.BenchmarkReport report) {
        StringBuilder sb = new StringBuilder();

        // 头部
        sb.append("benchmark_name,timestamp,repeat_count,warmup_seconds\n");
        sb.append(escapeCsv(report.benchmarkName())).append(",")
                .append(DATE_FORMATTER.format(report.timestamp())).append(",")
                .append(report.config().repeatCount()).append(",")
                .append(report.config().warmupDuration().getSeconds()).append("\n");

        sb.append("\n");

        // 环境信息
        BenchmarkBase.BenchmarkEnvironment env = report.environment();
        sb.append("os_name,os_version,java_version,jvm_name,jvm_args,processors,max_memory_mb,gc_collectors\n");
        sb.append(escapeCsv(env.osName())).append(",")
                .append(escapeCsv(env.osVersion())).append(",")
                .append(escapeCsv(env.javaVersion())).append(",")
                .append(escapeCsv(env.jvmName())).append(",")
                .append(escapeCsv(env.jvmArgs())).append(",")
                .append(env.availableProcessors()).append(",")
                .append(env.maxMemoryMB()).append(",")
                .append(escapeCsv(env.gcCollectors())).append("\n");

        sb.append("\n");

        // 统计数据
        BenchmarkBase.BenchmarkStatistics stats = report.statistics();
        sb.append("mean_throughput,std_throughput,cv_percent,min_throughput,max_throughput,");
        sb.append("p50_latency,p95_latency,p99_latency,p999_latency,max_latency\n");
        sb.append(String.format("%.2f", stats.meanThroughput())).append(",")
                .append(String.format("%.2f", stats.stdThroughput())).append(",")
                .append(String.format("%.2f", stats.cvThroughput())).append(",")
                .append(String.format("%.2f", stats.minThroughput())).append(",")
                .append(String.format("%.2f", stats.maxThroughput())).append(",")
                .append(stats.p50LatencyMs()).append(",")
                .append(stats.p95LatencyMs()).append(",")
                .append(stats.p99LatencyMs()).append(",")
                .append(stats.p999LatencyMs()).append(",")
                .append(stats.maxLatencyMs()).append("\n");

        sb.append("\n");

        // 运行数据
        sb.append("run_number,operation_count,elapsed_ms,throughput,memory_mb\n");
        int runNum = 1;
        for (BenchmarkBase.BenchmarkRunResult run : report.runs()) {
            sb.append(runNum++).append(",")
                    .append(run.operationCount()).append(",")
                    .append(run.elapsedMs()).append(",")
                    .append(String.format("%.2f", run.throughput())).append(",")
                    .append(run.memoryUsedMB()).append("\n");
        }

        return sb.toString();
    }

    /**
     * 生成 JSON 格式报告
     */
    public String generateJson(BenchmarkBase.BenchmarkReport report) {
        StringBuilder sb = new StringBuilder();

        sb.append("{\n");

        // 基本信息
        sb.append("  \"benchmarkName\": \"").append(escapeJson(report.benchmarkName())).append("\",\n");
        sb.append("  \"timestamp\": \"").append(DATE_FORMATTER.format(report.timestamp())).append("\",\n");

        // 配置
        sb.append("  \"config\": {\n");
        sb.append("    \"repeatCount\": ").append(report.config().repeatCount()).append(",\n");
        sb.append("    \"warmupSeconds\": ").append(report.config().warmupDuration().getSeconds()).append(",\n");
        sb.append("    \"recordLatencies\": ").append(report.config().recordLatencies()).append("\n");
        sb.append("  },\n");

        // 环境
        BenchmarkBase.BenchmarkEnvironment env = report.environment();
        sb.append("  \"environment\": {\n");
        sb.append("    \"osName\": \"").append(escapeJson(env.osName())).append("\",\n");
        sb.append("    \"osVersion\": \"").append(escapeJson(env.osVersion())).append("\",\n");
        sb.append("    \"javaVersion\": \"").append(escapeJson(env.javaVersion())).append("\",\n");
        sb.append("    \"jvmName\": \"").append(escapeJson(env.jvmName())).append("\",\n");
        sb.append("    \"jvmArgs\": \"").append(escapeJson(env.jvmArgs())).append("\",\n");
        sb.append("    \"processors\": ").append(env.availableProcessors()).append(",\n");
        sb.append("    \"maxMemoryMB\": ").append(env.maxMemoryMB()).append(",\n");
        sb.append("    \"gcCollectors\": \"").append(escapeJson(env.gcCollectors())).append("\"\n");
        sb.append("  },\n");

        // 统计
        BenchmarkBase.BenchmarkStatistics stats = report.statistics();
        sb.append("  \"statistics\": {\n");
        sb.append("    \"throughput\": {\n");
        sb.append("      \"mean\": ").append(String.format("%.2f", stats.meanThroughput())).append(",\n");
        sb.append("      \"stdDev\": ").append(String.format("%.2f", stats.stdThroughput())).append(",\n");
        sb.append("      \"cvPercent\": ").append(String.format("%.2f", stats.cvThroughput())).append(",\n");
        sb.append("      \"min\": ").append(String.format("%.2f", stats.minThroughput())).append(",\n");
        sb.append("      \"max\": ").append(String.format("%.2f", stats.maxThroughput())).append("\n");
        sb.append("    },\n");
        sb.append("    \"latencyMs\": {\n");
        sb.append("      \"p50\": ").append(stats.p50LatencyMs()).append(",\n");
        sb.append("      \"p95\": ").append(stats.p95LatencyMs()).append(",\n");
        sb.append("      \"p99\": ").append(stats.p99LatencyMs()).append(",\n");
        sb.append("      \"p99.9\": ").append(stats.p999LatencyMs()).append(",\n");
        sb.append("      \"max\": ").append(stats.maxLatencyMs()).append("\n");
        sb.append("    },\n");
        sb.append("    \"credibility\": {\n");
        sb.append("      \"isCredible\": ").append(stats.cvThroughput() < 10.0 && stats.outlierCount() == 0).append(",\n");
        sb.append("      \"cvThreshold\": 10.0,\n");
        sb.append("      \"actualCv\": ").append(String.format("%.2f", stats.cvThroughput())).append(",\n");
        sb.append("      \"outlierCount\": ").append(stats.outlierCount()).append("\n");
        sb.append("    }\n");
        sb.append("  },\n");

        // 运行数据
        sb.append("  \"runs\": [\n");
        List<BenchmarkBase.BenchmarkRunResult> runs = report.runs();
        for (int i = 0; i < runs.size(); i++) {
            BenchmarkBase.BenchmarkRunResult run = runs.get(i);
            sb.append("    {\n");
            sb.append("      \"runNumber\": ").append(i + 1).append(",\n");
            sb.append("      \"operationCount\": ").append(run.operationCount()).append(",\n");
            sb.append("      \"elapsedMs\": ").append(run.elapsedMs()).append(",\n");
            sb.append("      \"throughput\": ").append(String.format("%.2f", run.throughput())).append(",\n");
            sb.append("      \"memoryUsedMB\": ").append(run.memoryUsedMB()).append("\n");
            sb.append("    }");
            if (i < runs.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]\n");

        sb.append("}");

        return sb.toString();
    }

    /**
     * 保存报告到文件
     */
    public void saveReport(BenchmarkBase.BenchmarkReport report, Path outputDir,
                          ReportFormat... formats) throws IOException {
        Files.createDirectories(outputDir);

        String baseName = report.benchmarkName()
                .replaceAll("[^a-zA-Z0-9_-]", "_")
                .toLowerCase();

        for (ReportFormat format : formats) {
            String content;
            String extension;

            switch (format) {
                case MARKDOWN -> {
                    content = generateMarkdown(report);
                    extension = "md";
                }
                case CSV -> {
                    content = generateCsv(report);
                    extension = "csv";
                }
                case JSON -> {
                    content = generateJson(report);
                    extension = "json";
                }
                default -> throw new IllegalArgumentException("Unknown format: " + format);
            }

            Path filePath = outputDir.resolve(baseName + "." + extension);
            Files.writeString(filePath, content);
        }
    }

    /**
     * 转义 CSV 字段
     */
    private String escapeCsv(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    /**
     * 转义 JSON 字符串
     */
    private String escapeJson(String value) {
        if (value == null) return "";
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * 报告格式
     */
    public enum ReportFormat {
        MARKDOWN,
        CSV,
        JSON
    }
}
