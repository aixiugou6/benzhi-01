package com.risk;

import com.risk.config.RiskEngineConfig;
import com.risk.stream.AcknowledgedDecision;
import com.risk.stream.DecisionSink;
import com.risk.stream.RedisStreamsSource;
import com.risk.stream.RiskProcessFunction;
import com.risk.stream.RuleBroadcastDescriptor;
import com.risk.stream.RuleReloadSource;
import com.risk.stream.StreamTransaction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实时风控引擎常驻服务入口。
 *
 * <p>拓扑：
 * <pre>
 *  Redis Stream(risk.transactions) ──► keyBy(cardNo) ──┐
 *                                                        ├─ KeyedBroadcastProcess(规则引擎) ─► Redis Stream(risk.decisions)
 *  规则文件轮询(热更新) ──► 广播规则原文 ────────────────┘
 * </pre>
 *
 * <p>用法：
 * <pre>
 *   java -jar target/risk-engine.jar [config/application.json]
 * </pre>
 * 配置文件路径可省略（默认 {@code config/application.json}，不存在时读 classpath）。
 */
public class RiskEngineApplication {

    private static final Logger log = LoggerFactory.getLogger(RiskEngineApplication.class);

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : "config/application.json";
        RiskEngineConfig config = RiskEngineConfig.load(configPath);

        log.info("启动实时风控引擎: redis={}:{} db={}, 交易流={}, 决策流={}, 规则文件={}, 热更新间隔={}s",
                config.getRedisHost(), config.getRedisPort(), config.getRedisDatabase(),
                config.getTransactionStream(), config.getDecisionStream(),
                config.getRulesFile(), config.getRulesReloadSeconds());

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(config.getParallelism());
        buildTopology(env, config);

        env.execute("realtime-risk-engine");
    }

    /**
     * 装配风控拓扑（main 与端到端测试共用，保证测试跑的就是线上同一张拓扑）。
     */
    public static void buildTopology(StreamExecutionEnvironment env, RiskEngineConfig config) {

        // 交易事件流（Redis Streams 消费组）
        DataStream<StreamTransaction> transactions = env
                .addSource(new RedisStreamsSource(config),
                        TypeInformation.of(new TypeHint<StreamTransaction>() {
                        }))
                .name("redis-transaction-source")
                .uid("redis-transaction-source");

        // 规则热更新广播流（传输规则文件原文，算子本地编译）
        DataStream<String> ruleBroadcast = env
                .addSource(new RuleReloadSource(config.getRulesFile(),
                        config.getRulesReloadSeconds() * 1000L),
                        TypeInformation.of(String.class))
                .name("rule-reload-source")
                .uid("rule-reload-source");
        BroadcastStream<String> broadcastRules =
                ruleBroadcast.broadcast(RuleBroadcastDescriptor.RULESET_STATE);

        // 按卡号分区，连接广播规则后进入风控核心算子
        DataStream<AcknowledgedDecision> decisions = transactions
                .keyBy(t -> t.getEvent().getCardNo())
                .connect(broadcastRules)
                .process(new RiskProcessFunction(config))
                .name("risk-process")
                .uid("risk-process")
                .returns(TypeInformation.of(new TypeHint<AcknowledgedDecision>() {
                }));

        // 决策输出到 Redis Stream 并 ACK 交易条目
        decisions
                .addSink(new DecisionSink(config))
                .name("decision-sink")
                .uid("decision-sink");
    }
}
