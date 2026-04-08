package com.loomq.benchmark.framework;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 压测框架单元测试 (v0.4.3)
 *
 * 验证标准化压测框架的基本功能。
 *
 * @author loomq
 * @since v0.4.3
 */
@DisplayName("压测框架单元测试")
class BenchmarkFrameworkTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("内存态压测应生成有效报告")
    void testInMemoryBenchmark() {
        // 使用小规模快速测试（CI环境需要足够任务量才能产生非零统计）
        BenchmarkBase.BenchmarkConfig config = BenchmarkBase.BenchmarkConfig.defaultConfig()
                .withRepeatCount(2)
                .withWarmupDuration(java.time.Duration.ofMillis(500));

        InMemoryBenchmark benchmark = new InMemoryBenchmark(10000, config);
        BenchmarkBase.BenchmarkReport report = benchmark.execute();

        // 验证报告结构
        assertNotNull(report);
        assertNotNull(report.benchmarkName());
        assertNotNull(report.timestamp());
        assertNotNull(report.environment());
        assertNotNull(report.config());
        assertNotNull(report.runs());
        assertNotNull(report.statistics());

        // 验证运行次数
        assertEquals(2, report.runs().size());

        // 验证统计数据
        BenchmarkBase.BenchmarkStatistics stats = report.statistics();
        assertTrue(stats.meanThroughput() > 0, "平均吞吐应大于0");
        assertTrue(stats.p50LatencyMs() >= 0, "P50延迟应>=0");

        // 验证环境信息
        BenchmarkBase.BenchmarkEnvironment env = report.environment();
        assertNotNull(env.osName());
        assertNotNull(env.javaVersion());
        assertTrue(env.availableProcessors() > 0);
        assertTrue(env.maxMemoryMB() > 0);
    }

    @Test
    @DisplayName("报告生成器应生成有效格式")
    void testBenchmarkReporter() {
        BenchmarkBase.BenchmarkConfig config = BenchmarkBase.BenchmarkConfig.defaultConfig()
                .withRepeatCount(2);

        InMemoryBenchmark benchmark = new InMemoryBenchmark(500, config);
        BenchmarkBase.BenchmarkReport report = benchmark.execute();

        BenchmarkReporter reporter = new BenchmarkReporter();

        // 验证 Markdown 格式
        String markdown = reporter.generateMarkdown(report);
        assertNotNull(markdown);
        assertTrue(markdown.contains("# "));
        assertTrue(markdown.contains("## "));
        assertTrue(markdown.contains(report.benchmarkName()));

        // 验证 CSV 格式
        String csv = reporter.generateCsv(report);
        assertNotNull(csv);
        assertTrue(csv.contains(","));

        // 验证 JSON 格式
        String json = reporter.generateJson(report);
        assertNotNull(json);
        assertTrue(json.contains("{"));
        assertTrue(json.contains("}"));
    }

    @Test
    @DisplayName("可信结果判断应正确")
    void testCredibilityAssessment() {
        // 创建一个模拟的低变异系数统计
        BenchmarkBase.BenchmarkStatistics credibleStats = new BenchmarkBase.BenchmarkStatistics(
                1000.0,  // mean
                50.0,    // std
                5.0,     // cv = 5% < 10%
                900.0,   // min
                1100.0,  // max
                1, 2, 3, 4, 5,  // latencies
                0,       // outliers = 0
                5        // total runs
        );

        assertTrue(credibleStats.cvThroughput() < 10.0, "CV应小于10%");
        assertEquals(0, credibleStats.outlierCount(), "异常值应为0");

        // 创建一个高变异系数统计
        BenchmarkBase.BenchmarkStatistics nonCredibleStats = new BenchmarkBase.BenchmarkStatistics(
                1000.0,
                200.0,   // std = 200
                20.0,    // cv = 20% > 10%
                500.0,
                1500.0,
                1, 2, 3, 4, 5,
                2,       // outliers > 0
                5
        );

        assertTrue(nonCredibleStats.cvThroughput() >= 10.0, "CV应>=10%");
        assertTrue(nonCredibleStats.outlierCount() > 0, "应有异常值");
    }

    @Test
    @DisplayName("延迟记录器应正确记录")
    void testLatencyRecorder() {
        BenchmarkBase.LatencyRecorder recorder = new BenchmarkBase.LatencyRecorder();

        recorder.record(10);
        recorder.record(20);
        recorder.recordNano(30_000_000); // 30ms in ns

        assertEquals(3, recorder.size());

        var latencies = recorder.getLatencies();
        assertEquals(3, latencies.size());
        assertEquals(10, latencies.get(0));
        assertEquals(20, latencies.get(1));
        assertEquals(30, latencies.get(2)); // 30ns -> 30ms
    }

    @Test
    @DisplayName("吞吐计时器应正确计算")
    void testThroughputTimer() throws InterruptedException {
        BenchmarkBase.ThroughputTimer timer = new BenchmarkBase.ThroughputTimer();

        // 模拟操作
        for (int i = 0; i < 100; i++) {
            timer.increment();
        }
        timer.add(50);

        // 等待一小段时间
        Thread.sleep(10);

        double throughput = timer.getThroughput();
        long elapsedMs = timer.getElapsedMs();

        assertTrue(throughput > 0, "吞吐应大于0");
        assertTrue(elapsedMs >= 10, "耗时应>=10ms");
        assertEquals(150, timer.getCount());
    }
}
