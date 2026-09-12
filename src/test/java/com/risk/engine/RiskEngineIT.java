package com.risk.engine;

import com.risk.RedisIntegrationTest;
import com.risk.model.Decision;
import com.risk.model.DecisionAction;
import com.risk.model.ReasonCode;
import com.risk.model.TransactionEvent;
import com.risk.rule.RuleCompiler;
import com.risk.rule.RuleSetHolder;
import com.risk.window.RedisWindowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 模块三/四：规则求值与优先级、决策结果与原因码（真实 Redis + 可控时钟）。
 */
class RiskEngineIT extends RedisIntegrationTest {

    private RuleSetHolder holder;
    private RedisWindowStore windowStore;
    private RuleCompiler compiler;

    /** 两个规则：金额规则优先级 5（先判），高频规则优先级 10（后判）。 */
    private static final String RULES = """
            {
              "version": "test-v1",
              "windows": [ { "alias": "w1", "keyBy": "cardNo", "windowMs": 60000 } ],
              "rules": [
                { "id": "amount", "enabled": true, "priority": 5,
                  "reasonCode": "R_AMOUNT_EXCEEDED",
                  "description": "单笔金额异常", "condition": "$amount >= 50000" },
                { "id": "freq", "enabled": true, "priority": 10,
                  "reasonCode": "R_HIGH_FREQUENCY",
                  "description": "短时高频", "condition": "count(w1) >= 5" }
              ]
            }
            """;

    @BeforeEach
    void setUp() {
        flushDb();
        holder = new RuleSetHolder();
        windowStore = new RedisWindowStore(pool);
        compiler = new RuleCompiler();
    }

    private RiskEngine engineAt(long t0) {
        return new RiskEngine(holder, windowStore, new ScriptedClock(t0));
    }

    private TransactionEvent txn(String id, String card, double amount, String type, long t) {
        return new TransactionEvent(id, card, amount, "CNY", "M-1", type, t);
    }

    @Test
    void 前四笔放行_第五笔短时高频拦截并带原因码与计数() throws Exception {
        holder.set(compiler.compileJson(RULES, "test"));
        long t0 = 1_000_000L;
        RiskEngine engine = engineAt(t0);

        for (int i = 1; i <= 4; i++) {
            Decision d = engine.evaluate(txn("e" + i, "CARD-A", 100, "PURCHASE", t0 + i * 1000));
            assertEquals(DecisionAction.PASS, d.getAction(), "第 " + i + " 笔应放行");
            assertEquals("test-v1", d.getRuleSetVersion());
        }
        Decision fifth = engine.evaluate(txn("e5", "CARD-A", 100, "PURCHASE", t0 + 5_000));
        assertEquals(DecisionAction.BLOCK, fifth.getAction());
        assertEquals("R_HIGH_FREQUENCY", fifth.getReasonCode());
        assertEquals("freq", fifth.getMatchedRuleId());
        assertEquals(10, fifth.getPriority());
        assertEquals(5L, fifth.getWindowCount());
        assertNotNull(fifth.getReason());
    }

    @Test
    void 优先级_金额规则先于高频命中() throws Exception {
        holder.set(compiler.compileJson(RULES, "test"));
        RiskEngine engine = engineAt(2_000_000L);
        // 同卡第 5 笔且金额超限：两条都满足，priority=5 的金额规则必须胜出
        for (int i = 1; i <= 4; i++) {
            engine.evaluate(txn("p" + i, "CARD-B", 100, "PURCHASE", 2_000_000L + i * 1000));
        }
        Decision d = engine.evaluate(txn("p5", "CARD-B", 60_000, "PURCHASE", 2_005_000L));
        assertEquals(DecisionAction.BLOCK, d.getAction());
        assertEquals("amount", d.getMatchedRuleId());
        assertEquals("R_AMOUNT_EXCEEDED", d.getReasonCode());
    }

    @Test
    void 超过窗口后计数滑出_仍然放行() throws Exception {
        holder.set(compiler.compileJson(RULES, "test"));
        long t0 = 5_000_000L;
        // 时钟随每笔推进：前 4 笔间隔 1s，第 5 笔在 t0+62s（窗口 60s）：第 1 笔已滑出
        RiskEngine engine = new RiskEngine(holder, windowStore, new ScriptedClock(
                t0 + 1_000, t0 + 2_000, t0 + 3_000, t0 + 4_000, t0 + 62_000));
        for (int i = 1; i <= 4; i++) {
            engine.evaluate(txn("s" + i, "CARD-C", 100, "PURCHASE", t0 + i * 1_000L));
        }
        Decision d = engine.evaluate(txn("s5", "CARD-C", 100, "PURCHASE", t0 + 62_000L));
        assertEquals(DecisionAction.PASS, d.getAction());
    }

    @Test
    void 不同卡号窗口互不干扰() throws Exception {
        holder.set(compiler.compileJson(RULES, "test"));
        RiskEngine engine = engineAt(7_000_000L);
        for (int i = 1; i <= 5; i++) {
            engine.evaluate(txn("x" + i, "CARD-D", 100, "PURCHASE", 7_000_000L + i * 1000));
        }
        Decision other = engine.evaluate(txn("y1", "CARD-E", 100, "PURCHASE", 7_006_000L));
        assertEquals(DecisionAction.PASS, other.getAction());
    }

    @Test
    void 非法事件_负金额_返回INVALID且不触碰规则() {
        RiskEngine realEngine = new RiskEngine(new RuleSetHolder(), windowStore, () -> 9_000_000L);
        Decision d = realEngine.evaluate(txn("bad", "CARD-F", -1, "PURCHASE", 1L));
        assertEquals(DecisionAction.INVALID, d.getAction());
        assertEquals(ReasonCode.SYS_INVALID_EVENT.name(), d.getReasonCode());
    }

    @Test
    void 规则集未加载_failClosed拦截() {
        RiskEngine engine = new RiskEngine(new RuleSetHolder(), windowStore, () -> 10_000_000L);
        Decision d = engine.evaluate(txn("g1", "CARD-G", 100, "PURCHASE", 1L));
        assertEquals(DecisionAction.BLOCK, d.getAction());
        assertEquals(ReasonCode.SYS_ENGINE_ERROR.name(), d.getReasonCode());
    }

    @Test
    void 分组字段缺失导致无法统计_failClosed而非误放行() throws Exception {
        String rules = """
                { "version": "v",
                  "windows": [ { "alias": "wm", "keyBy": "merchantId", "windowMs": 60000 } ],
                  "rules": [ { "id": "m", "priority": 1, "reasonCode": "R_HIGH_FREQUENCY",
                               "condition": "count(wm) >= 2" } ] }
                """;
        holder.set(compiler.compileJson(rules, "test"));
        RiskEngine engine = new RiskEngine(holder, windowStore, () -> 11_000_000L);
        // merchantId 为 null
        TransactionEvent e = new TransactionEvent("m1", "CARD-H", 100, "CNY", null, "PURCHASE", 1L);
        Decision d = engine.evaluate(e);
        assertEquals(DecisionAction.BLOCK, d.getAction());
        assertEquals(ReasonCode.SYS_ENGINE_ERROR.name(), d.getReasonCode());
    }

    @Test
    void 放行决策不携带命中规则与计数() throws Exception {
        holder.set(compiler.compileJson(RULES, "test"));
        RiskEngine engine = new RiskEngine(holder, windowStore, () -> 12_000_000L);
        Decision d = engine.evaluate(txn("ok1", "CARD-I", 100, "PURCHASE", 1L));
        assertEquals(DecisionAction.PASS, d.getAction());
        assertNull(d.getMatchedRuleId());
        assertNull(d.getWindowCount());
    }
}
