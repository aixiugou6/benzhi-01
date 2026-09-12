package com.risk.stream;

import com.risk.config.RiskEngineConfig;
import com.risk.engine.Clock;
import com.risk.engine.RiskEngine;
import com.risk.model.Decision;
import com.risk.rule.RuleCompiler;
import com.risk.rule.RuleSetHolder;
import com.risk.rule.expr.RuleParseException;
import com.risk.rule.model.CompiledRuleSet;
import com.risk.window.RedisWindowStore;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import com.risk.redis.JedisFactory;

/**
 * 风控核心算子（按卡号 keyBy）。
 *
 * <ul>
 *   <li>广播流：规则文件原文 → 本地编译 → 成功才替换 {@link RuleSetHolder} 并写广播状态；
 *       坏规则编译失败则保留旧版，交易处理不受影响（热更新安全网）。</li>
 *   <li>交易流：调用 {@link RiskEngine}，输出 {@link AcknowledgedDecision} 给 Sink。</li>
 * </ul>
 */
public class RiskProcessFunction
        extends KeyedBroadcastProcessFunction<String, StreamTransaction, String, AcknowledgedDecision> {

    private static final Logger log = LoggerFactory.getLogger(RiskProcessFunction.class);

    private final RiskEngineConfig config;

    private transient JedisPool pool;
    private transient RedisWindowStore windowStore;
    private transient RuleSetHolder holder;
    private transient RuleCompiler compiler;
    private transient RiskEngine engine;

    public RiskProcessFunction(RiskEngineConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        pool = JedisFactory.create(config);
        windowStore = new RedisWindowStore(pool);
        holder = new RuleSetHolder();
        compiler = new RuleCompiler();
        engine = new RiskEngine(holder, windowStore, Clock.system());

        // 冷启动兜底：先从本地规则文件加载一次（广播的首次下发很快也会到达并覆盖）
        try {
            java.nio.file.Path p = java.nio.file.Path.of(config.getRulesFile());
            if (java.nio.file.Files.exists(p)) {
                holder.set(compiler.compileFile(p));
                log.info("启动加载本地规则文件成功: version={}", holder.version());
            }
        } catch (Exception e) {
            log.warn("启动加载本地规则文件失败，等待广播下发: {}", e.getMessage());
        }
    }

    @Override
    public void processBroadcastElement(String rulesJson, Context ctx,
                                        Collector<AcknowledgedDecision> out) throws Exception {
        try {
            CompiledRuleSet next = compiler.compileJson(rulesJson, "broadcast");
            BroadcastState<String, String> state =
                    ctx.getBroadcastState(RuleBroadcastDescriptor.RULESET_STATE);
            state.put(RuleBroadcastDescriptor.RULESET_KEY, rulesJson);
            holder.set(next);
            log.info("算子 {} 规则热更新生效: version={}, 规则数={}",
                    getRuntimeContext().getIndexOfThisSubtask(), next.version(), next.rules().size());
        } catch (RuleParseException | java.io.IOException e) {
            // 坏规则整版拒绝，保留旧版继续服务
            log.error("收到非法规则文件，拒绝热更新，线上保留 version={}: {}",
                    holder.version(), e.getMessage());
        }
    }

    @Override
    public void processElement(StreamTransaction txn, ReadOnlyContext ctx,
                               Collector<AcknowledgedDecision> out) throws Exception {
        if (holder.get() == null) {
            // 广播尚未到达时的恢复路径：从广播状态（故障恢复后）取规则本地编译
            ReadOnlyBroadcastState<String, String> state =
                    ctx.getBroadcastState(RuleBroadcastDescriptor.RULESET_STATE);
            String json = state.get(RuleBroadcastDescriptor.RULESET_KEY);
            if (json != null) {
                holder.set(compiler.compileJson(json, "broadcast-state"));
            }
        }

        Decision decision = engine.evaluate(txn.getEvent());
        out.collect(new AcknowledgedDecision(txn.getEntryId(), decision));
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }
}
