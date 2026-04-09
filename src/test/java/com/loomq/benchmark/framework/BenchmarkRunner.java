package com.loomq.benchmark.framework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 标准化压测运行器 (v0.4.3)
 *
 * 运行所有维度的压测并生成统一报告。
 *
 * 使用方法:
 * ```bash
 * # 运行所有压测
 * java -cp target/test-classes com.loomq.benchmark.framework.BenchmarkRunner
 *
 * # 指定输出目录
 * java -cp target/test-classes com.loomq.benchmark.framework.BenchmarkRunner --output ./reports
 *
 * # 指定任务规模
 * java -cp target/test-classes com.loomq.benchmark.framework.BenchmarkRunner --scale 100000
 *
 * # 指定重复次数
 * java -cp target/test-classes com.loomq.benchmark.framework.BenchmarkRunner --repeat 10
 * ```
 *
 * @author loomq
 * @since v0.4.3
 */
public class BenchmarkRunner {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkRunner.class);

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    // 默认配置
    private int taskScale = 100_000;
    private int repeatCount = 5;
    private Path outputDir = Paths.get("benchmark-reports");

    public static void main(String[] args) throws Exception {
        BenchmarkRunner runner = new BenchmarkRunner();
        runner.parseArgs(args);
        runner.runAll();
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--output", "-o" -> outputDir = Paths.get(args[++i]);
                case "--scale", "-s" -> taskScale = Integer.parseInt(args[++i]);
                case "--repeat", "-r" -> repeatCount = Integer.parseInt(args[++i]);
                case "--help", "-h" -> {
                    printHelp();
                    System.exit(0);
                }
            }
        }
    }

    private void printHelp() {
        System.out.println("LoomQ v0.4.3 标准化压测运行器");
        System.out.println();
        System.out.println("用法: BenchmarkRunner [选项]");
        System.out.println();
        System.out.println("选项:");
        System.out.println("  --output, -o <路径>   报告输出目录 (默认: ./benchmark-reports)");
        System.out.println("  --scale, -s <数量>    任务规模 (默认: 100000)");
        System.out.println("  --repeat, -r <次数>   重复次数 (默认: 5)");
        System.out.println("  --help, -h            显示帮助信息");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  BenchmarkRunner --scale 1000000 --repeat 10");
        System.out.println("  BenchmarkRunner -o ./reports -s 500000 -r 5");
    }

    public void runAll() throws Exception {
        // 创建输出目录
        String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
        Path runDir = outputDir.resolve("run-" + timestamp);
        Files.createDirectories(runDir);

        logger.info("╔════════════════════════════════════════════════════════════╗");
        logger.info("║     LoomQ v0.4.3 标准化性能压测套件                      ║");
        logger.info("╚════════════════════════════════════════════════════════════╝");
        logger.info("");
        logger.info("配置参数:");
        logger.info("  任务规模: {}", formatNumber(taskScale));
        logger.info("  重复次数: {}", repeatCount);
        logger.info("  输出目录: {}", runDir);
        logger.info("");

        // 压测配置
        BenchmarkBase.BenchmarkConfig config = BenchmarkBase.BenchmarkConfig.defaultConfig()
                .withRepeatCount(repeatCount);

        // 运行所有压测
        List<BenchmarkBase.BenchmarkReport> reports = new ArrayList<>();

        // 1. 内存态压测
        logger.info("========================================");
        logger.info("[1/4] 内存态压测");
        logger.info("========================================");
        try {
            InMemoryBenchmark memBenchmark = new InMemoryBenchmark(taskScale, config);
            reports.add(memBenchmark.execute());
        } catch (Exception e) {
            logger.error("内存态压测失败", e);
        }

        // 2. WAL 态压测
        logger.info("");
        logger.info("========================================");
        logger.info("[2/4] WAL 态压测");
        logger.info("========================================");
        try {
            WalBenchmark walBenchmark = new WalBenchmark(taskScale, config);
            reports.add(walBenchmark.execute());
        } catch (Exception e) {
            logger.error("WAL 态压测失败", e);
        }

        // 3. 恢复态压测（较小规模）
        int recoveryScale = Math.min(taskScale / 10, 50_000);
        logger.info("");
        logger.info("========================================");
        logger.info("[3/4] 恢复态压测 (规模: {})", formatNumber(recoveryScale));
        logger.info("========================================");
        try {
            RecoveryBenchmark recoveryBenchmark = new RecoveryBenchmark(recoveryScale, config);
            reports.add(recoveryBenchmark.execute());
        } catch (Exception e) {
            logger.error("恢复态压测失败", e);
        }

        // 4. 端到端压测（较小规模）
        int e2eScale = Math.min(taskScale / 5, 100_000);
        logger.info("");
        logger.info("========================================");
        logger.info("[4/4] 端到端压测 (规模: {})", formatNumber(e2eScale));
        logger.info("========================================");
        try {
            EndToEndBenchmark e2eBenchmark = new EndToEndBenchmark(e2eScale, config);
            reports.add(e2eBenchmark.execute());
        } catch (Exception e) {
            logger.error("端到端压测失败", e);
        }

        // 生成报告
        logger.info("");
        logger.info("========================================");
        logger.info("生成报告");
        logger.info("========================================");

        BenchmarkReporter reporter = new BenchmarkReporter();

        for (BenchmarkBase.BenchmarkReport report : reports) {
            reporter.saveReport(report, runDir,
                    BenchmarkReporter.ReportFormat.MARKDOWN,
                    BenchmarkReporter.ReportFormat.CSV,
                    BenchmarkReporter.ReportFormat.JSON);
            logger.info("已生成报告: {}", report.benchmarkName());
        }

        // 生成综合报告
        generateSummaryReport(reports, runDir);

        logger.info("");
        logger.info("╔════════════════════════════════════════════════════════════╗");
        logger.info("║                    所有压测已完成                          ║");
        logger.info("║         报告目录: {}              ║",
                String.format("%-40s", runDir.toString()).substring(0, Math.min(40, runDir.toString().length())));
        logger.info("╚════════════════════════════════════════════════════════════╝");
    }

    /**
     * 生成综合报告
     */
    private void generateSummaryReport(List<BenchmarkBase.BenchmarkReport> reports, Path outputDir)
            throws IOException {

        StringBuilder md = new StringBuilder();
        md.append("# LoomQ v0.4.3 性能压测综合报告\n\n");
        md.append("**生成时间**: ").append(LocalDateTime.now().toString()).append("\n\n");

        // 环境信息（取第一个报告的环境）
        if (!reports.isEmpty()) {
            BenchmarkBase.BenchmarkEnvironment env = reports.get(0).environment();
            md.append("## 测试环境\n\n");
            md.append("| 属性 | 值 |\n");
            md.append("|------|-----|\n");
            md.append("| 操作系统 | ").append(env.osName()).append(" ").append(env.osVersion()).append(" |\n");
            md.append("| JDK版本 | ").append(env.javaVersion()).append(" |\n");
            md.append("| JVM参数 | ").append(env.jvmArgs()).append(" |\n");
            md.append("| 处理器 | ").append(env.availableProcessors()).append(" 核 |\n");
            md.append("| 内存 | ").append(env.maxMemoryMB()).append(" MB |\n");
            md.append("| GC收集器 | ").append(env.gcCollectors()).append(" |\n");
            md.append("\n");
        }

        // 结果汇总表
        md.append("## 压测结果汇总\n\n");
        md.append("| 压测类型 | 平均吞吐(ops/s) | CV(%) | P95延迟(ms) | 可信度 |\n");
        md.append("|----------|-----------------|-------|-------------|--------|\n");

        int credibleCount = 0;
        for (BenchmarkBase.BenchmarkReport report : reports) {
            BenchmarkBase.BenchmarkStatistics stats = report.statistics();
            boolean credible = stats.cvThroughput() < 10.0 && stats.outlierCount() == 0;
            if (credible) credibleCount++;

            md.append("| ").append(report.benchmarkName()).append(" | ");
            md.append(String.format("%.2f", stats.meanThroughput())).append(" | ");
            md.append(String.format("%.2f", stats.cvThroughput())).append(" | ");
            md.append(stats.p95LatencyMs()).append(" | ");
            md.append(credible ? "✅ 可信" : "⚠️ 存疑").append(" |\n");
        }
        md.append("\n");

        // 可信度统计
        md.append("## 可信度评估\n\n");
        md.append("- **可信测试**: ").append(credibleCount).append("/").append(reports.size()).append("\n");
        md.append("- **标准要求**: 变异系数(CV) < 10%\n\n");

        if (credibleCount == reports.size()) {
            md.append("✅ **所有测试结果可信**\n\n");
        } else {
            md.append("⚠️ **部分测试结果存疑，建议增加重复次数或检查系统稳定性**\n\n");
        }

        // 关键发现
        md.append("## 关键发现\n\n");

        // 最高吞吐
        reports.stream()
                .max((a, b) -> Double.compare(a.statistics().meanThroughput(),
                        b.statistics().meanThroughput()))
                .ifPresent(r -> md.append("- **最高吞吐**: ")
                        .append(String.format("%.2f", r.statistics().meanThroughput()))
                        .append(" ops/s (").append(r.benchmarkName()).append(")\n"));

        // 最低延迟
        reports.stream()
                .filter(r -> r.statistics().p95LatencyMs() > 0)
                .min((a, b) -> Long.compare(a.statistics().p95LatencyMs(),
                        b.statistics().p95LatencyMs()))
                .ifPresent(r -> md.append("- **最低P95延迟**: ")
                        .append(r.statistics().p95LatencyMs())
                        .append(" ms (").append(r.benchmarkName()).append(")\n"));

        md.append("\n");

        // 详细报告链接
        md.append("## 详细报告\n\n");
        for (BenchmarkBase.BenchmarkReport report : reports) {
            String fileName = report.benchmarkName()
                    .replaceAll("[^a-zA-Z0-9_-]", "_")
                    .toLowerCase();
            md.append("- [").append(report.benchmarkName()).append("](")
                    .append(fileName).append(".md)\n");
        }
        md.append("\n");

        // 测试局限
        md.append("## 测试局限\n\n");
        md.append("1. 测试结果受硬件配置和系统负载影响\n");
        md.append("2. JVM 热身状态可能影响首次运行结果\n");
        md.append("3. 未测试分布式部署场景\n");
        md.append("4. 长期稳定性未验证\n");
        md.append("\n");

        // 结论
        md.append("## 结论\n\n");
        md.append("基于本次测试，LoomQ 在当前环境下表现出：\n\n");

        double avgThroughput = reports.stream()
                .mapToDouble(r -> r.statistics().meanThroughput())
                .average()
                .orElse(0.0);

        md.append("- **平均吞吐能力**: ").append(String.format("%.2f", avgThroughput)).append(" ops/s\n");
        md.append("- **结果可信度**: ").append(credibleCount == reports.size() ? "高" : "中等").append("\n");

        Files.writeString(outputDir.resolve("SUMMARY.md"), md.toString());
        logger.info("已生成综合报告: SUMMARY.md");
    }

    private String formatNumber(int n) {
        if (n >= 1_000_000) return (n / 1_000_000) + "M";
        if (n >= 1_000) return (n / 1_000) + "K";
        return String.valueOf(n);
    }
}
