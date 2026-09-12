package com.risk.e2e;

import com.risk.RedisIntegrationTest;
import com.risk.RiskEngineApplication;
import com.risk.config.RiskEngineConfig;
import com.risk.io.JsonCodecs;
import com.risk.model.Decision;
import com.risk.model.TransactionEvent;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.resps.StreamEntry;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 模块五（端到端）：Flink MiniCluster 常驻拓扑 + 真实 Redis Streams + 规则热更新。
 *
 * <p>验证：交易 XADD → 引擎消费 → 决策 XADD 到决策流并 XACK；
 * 运行中改规则文件不重启即生效；坏规则整版拒绝、线上保留旧版；修正后再次热更生效。
 */
class HotReloadEndToEndIT extends RedisIntegrationTest {

    private String rulesJson(int threshold, String version) {
        return """
                {
                  "version": "%s",
                  "windows": [ { "alias": "w1", "keyBy": "cardNo", "windowMs": 60000 } ],
                  "rules": [
                    { "id": "freq", "enabled": true, "priority": 10,
                      "reasonCode": "R_HIGH_FREQUENCY",
                      "description": "短时高频",
                      "condition": "count(w1) >= %d" }
                  ]
                }
                """.formatted(version, threshold);
    }

    @Test
    void 常驻服务消费交易流_运行中热更新规则_无需重启() throws Exception {
        flushDb();

        Path rulesFile = Files.createTempFile("rules-", ".json");
        Files.writeString(rulesFile, rulesJson(3, "v1"), StandardCharsets.UTF_8);

        RiskEngineConfig cfg = newConfig("risk-engine-e2e", rulesFile);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        RiskEngineApplication.buildTopology(env, cfg);
        JobClient job = env.executeAsync("risk-e2e");

        try {
            waitForSource(cfg);

            // ---- v1 阈值=3：前两笔放行，第三笔拦截 ----
            assertEquals("PASS", sendAndWait(cfg, "t1", "CARD-A", 100).getAction().name());
            assertEquals("PASS", sendAndWait(cfg, "t2", "CARD-A", 100).getAction().name());
            Decision d3 = sendAndWait(cfg, "t3", "CARD-A", 100);
            assertEquals("BLOCK", d3.getAction().name());
            assertEquals("R_HIGH_FREQUENCY", d3.getReasonCode());
            assertEquals("v1", d3.getRuleSetVersion());
            // 决策落库后交易条目已 ACK：PEL 应为 0
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> pendingCount(cfg) == 0);

            // ---- 热更新 v2：阈值放宽到 10（用独立卡验证语义）----
            Files.writeString(rulesFile, rulesJson(10, "v2"), StandardCharsets.UTF_8);
            awaitVersion(cfg, "v2");                       // 等广播真正生效
            // 新卡连发 5 笔：v1 下第 3 笔会拦，v2 下全部放行
            for (int i = 1; i <= 5; i++) {
                Decision d = sendAndWait(cfg, "r2-" + i, "CARD-V2", 100);
                assertEquals("PASS", d.getAction().name(), "v2 阈值 10，第 " + i + " 笔应放行");
                assertEquals("v2", d.getRuleSetVersion());
            }

            // ---- 写入坏规则：整版拒绝，线上保留 v2 ----
            Files.writeString(rulesFile, "{ this is : not-json ,,", StandardCharsets.UTF_8);
            Thread.sleep(1_500);                          // 超过 1s 轮询间隔，确保被读到
            Decision dbad = sendAndWait(cfg, "bad-1", "CARD-BAD", 100);
            assertEquals("PASS", dbad.getAction().name());
            assertEquals("v2", dbad.getRuleSetVersion(), "坏规则必须被拒绝，继续跑 v2");

            // ---- 修正为 v3：阈值收紧到 5，热更重新生效 ----
            Files.writeString(rulesFile, rulesJson(5, "v3"), StandardCharsets.UTF_8);
            awaitVersion(cfg, "v3");
            for (int i = 1; i <= 4; i++) {
                assertEquals("PASS", sendAndWait(cfg, "r3-" + i, "CARD-V3", 100).getAction().name());
            }
            Decision d5 = sendAndWait(cfg, "r3-5", "CARD-V3", 100);
            assertEquals("BLOCK", d5.getAction().name());
            assertEquals("v3", d5.getRuleSetVersion());
            assertEquals(5L, d5.getWindowCount());
        } catch (Throwable t) {
            job.cancel().get(10, TimeUnit.SECONDS);
            Files.deleteIfExists(rulesFile);
            throw t;
        }
        job.cancel().get(10, TimeUnit.SECONDS);
        Files.deleteIfExists(rulesFile);
    }

    @Test
    void 毒消息被丢弃并ACK_不阻塞后续交易() throws Exception {
        flushDb();

        Path rulesFile = Files.createTempFile("rules-poison-", ".json");
        Files.writeString(rulesFile, rulesJson(100, "v-poison"), StandardCharsets.UTF_8);
        RiskEngineConfig cfg = newConfig("risk-engine-e2e-poison", rulesFile);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        RiskEngineApplication.buildTopology(env, cfg);
        JobClient job = env.executeAsync("risk-e2e-poison");

        try {
            waitForSource(cfg);
            try (Jedis jedis = pool.getResource()) {
                jedis.xadd(cfg.getTransactionStream(), XAddParams.xAddParams(),
                        Map.of("json", "---not json---"));
            }
            // 毒消息被 ACK 丢弃（PEL=0），后续正常消息照常处理
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> pendingCount(cfg) == 0);
            Decision ok = sendAndWait(cfg, "ok1", "CARD-P", 10);
            assertEquals("PASS", ok.getAction().name());
        } catch (Throwable t) {
            job.cancel().get(10, TimeUnit.SECONDS);
            Files.deleteIfExists(rulesFile);
            throw t;
        }
        job.cancel().get(10, TimeUnit.SECONDS);
        Files.deleteIfExists(rulesFile);
    }

    // ------------------------------------------------------------------

    private RiskEngineConfig newConfig(String group, Path rulesFile) {
        RiskEngineConfig cfg = new RiskEngineConfig();
        cfg.setRedisHost(config.getRedisHost());
        cfg.setRedisPort(config.getRedisPort());
        cfg.setConsumerGroup(group);
        cfg.setRulesFile(rulesFile.toString());
        cfg.setRulesReloadSeconds(1);
        cfg.setParallelism(1);
        return cfg;
    }

    private void waitForSource(RiskEngineConfig cfg) {
        Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try (Jedis jedis = pool.getResource()) {
                        return jedis.exists(cfg.getTransactionStream());
                    }
                });
    }

    /** 反复发探针交易（独立探针卡），直到决策使用的规则版本变为期望版本。 */
    private void awaitVersion(RiskEngineConfig cfg, String expectedVersion) {
        AtomicInteger i = new AtomicInteger();
        Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS)
                .until(() -> {
                    String id = "probe-" + expectedVersion + "-" + i.incrementAndGet();
                    sendTxn(cfg, id, "PROBE-" + expectedVersion, 100);
                    Decision d = waitDecision(cfg, id, 5);
                    return d != null && expectedVersion.equals(d.getRuleSetVersion());
                });
    }

    private Decision sendAndWait(RiskEngineConfig cfg, String eventId, String card, double amount) {
        sendTxn(cfg, eventId, card, amount);
        Decision d = waitDecision(cfg, eventId, 15);
        assertTrue(d != null, "超时未收到 " + eventId + " 的决策");
        return d;
    }

    private void sendTxn(RiskEngineConfig cfg, String eventId, String card, double amount) {
        String json = JsonCodecs.toJson(
                new TransactionEvent(eventId, card, amount, "CNY", "M-1", "PURCHASE", null));
        try (Jedis jedis = pool.getResource()) {
            jedis.xadd(cfg.getTransactionStream(), XAddParams.xAddParams(), Map.of("json", json));
        }
    }

    private Decision waitDecision(RiskEngineConfig cfg, String eventId, int timeoutSec) {
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (System.currentTimeMillis() < deadline) {
            try (Jedis jedis = pool.getResource()) {
                for (StreamEntry e : jedis.xrange(cfg.getDecisionStream(), "-", "+")) {
                    Decision d = JsonCodecs.MAPPER.readValue(e.getFields().get("json"), Decision.class);
                    if (eventId.equals(d.getEventId())) {
                        return d;
                    }
                }
            } catch (Exception ignored) {
                // 重试
            }
            try {
                Thread.sleep(150);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    private long pendingCount(RiskEngineConfig cfg) {
        try (Jedis jedis = pool.getResource()) {
            var summary = jedis.xpending(cfg.getTransactionStream(), cfg.getConsumerGroup());
            return summary == null ? 0 : summary.getTotal();
        } catch (Exception e) {
            return -1;
        }
    }
}
