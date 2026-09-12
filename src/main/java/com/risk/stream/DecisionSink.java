package com.risk.stream;

import com.risk.config.RiskEngineConfig;
import com.risk.io.JsonCodecs;
import com.risk.redis.JedisFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XAddParams;

import java.util.Map;

/**
 * 决策 Sink：
 * <ol>
 *   <li>决策 JSON 写入决策 Redis Stream（近似 MAXLEN 裁剪，防止无界增长）；</li>
 *   <li>写成功后再 XACK 交易流对应条目——崩溃窗口外保证“决策落库才确认消费”（at-least-once）。</li>
 * </ol>
 */
public class DecisionSink extends RichSinkFunction<AcknowledgedDecision> {

    private static final Logger log = LoggerFactory.getLogger(DecisionSink.class);

    private final RiskEngineConfig config;

    private transient JedisPool pool;

    public DecisionSink(RiskEngineConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        pool = JedisFactory.create(config);
    }

    @Override
    public void invoke(AcknowledgedDecision item, Context context) {
        String json = JsonCodecs.toJson(item.getDecision());
        try (Jedis jedis = pool.getResource()) {
            jedis.xadd(config.getDecisionStream(),
                    XAddParams.xAddParams().maxLen(config.getDecisionStreamMaxLen()).approximateTrimming(),
                    Map.of("json", json));
            jedis.xack(config.getTransactionStream(), config.getConsumerGroup(), item.getSourceEntryId());
        }
        log.info("决策输出: {}", json);
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }
}
