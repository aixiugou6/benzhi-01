package com.risk.stream;

import com.risk.config.RiskEngineConfig;
import com.risk.io.JsonCodecs;
import com.risk.model.TransactionEvent;
import com.risk.redis.JedisFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAutoClaimParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;

/**
 * 交易事件 Source：以消费者组方式读取交易 Redis Stream。
 *
 * <p>投递语义 at-least-once：
 * <ul>
 *   <li>本 source <b>不</b> XACK 正常消息；消息随事件流带到下游，由决策 Sink 在决策写入后 ACK；</li>
 *   <li>崩溃未 ACK 的消息留在 PEL，通过周期性 XAUTOCLAIM（空闲超过 {@link #CLAIM_MIN_IDLE_MS}）
 *       重新领取处理；</li>
 *   <li>滑动窗口 Lua 以 eventId 幂等去重，重投不会重复计数；最坏情况是决策流出现重复决策行，
 *       下游按 eventId 去重即可。</li>
 *   <li>无法解析的毒消息直接 ACK 丢弃并记录日志，防止卡死分区。</li>
 * </ul>
 */
public class RedisStreamsSource extends RichParallelSourceFunction<StreamTransaction> {

    private static final Logger log = LoggerFactory.getLogger(RedisStreamsSource.class);

    /** PEL 中的消息空闲超过该时间才允许重新领取（毫秒）。 */
    private static final int CLAIM_MIN_IDLE_MS = 30_000;

    private final RiskEngineConfig config;

    private transient volatile boolean running = true;
    private transient JedisPool pool;

    public RedisStreamsSource(RiskEngineConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        pool = JedisFactory.create(config);
        try (Jedis jedis = pool.getResource()) {
            // MKSTREAM：流不存在时自动创建；起点 $（Jedis 中为 LAST_ENTRY）= 只消费组创建之后的新消息
            jedis.xgroupCreate(config.getTransactionStream(), config.getConsumerGroup(),
                    StreamEntryID.LAST_ENTRY, true);
            log.info("消费组 {} 创建于 stream {}", config.getConsumerGroup(),
                    config.getTransactionStream());
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
            log.info("消费组 {} 已存在，直接加入", config.getConsumerGroup());
        }
    }

    @Override
    public void run(SourceContext<StreamTransaction> ctx) throws Exception {
        // 不能依赖字段初始化器：该函数对象会被 Flink 序列化后反序列化分发，
        // transient 字段反序列化为默认值 false，必须在 run 入口显式置位。
        running = true;
        String consumer = "flink-" + getRuntimeContext().getIndexOfThisSubtask();
        log.info("交易 source 启动 consumer={}", consumer);
        while (running) {
            claimIdle(ctx, consumer);
            readNew(ctx, consumer);
        }
    }

    /** 领取并重新下发其他（或崩溃的）消费者遗留的空闲消息。 */
    private void claimIdle(SourceContext<StreamTransaction> ctx, String consumer) {
        try (Jedis jedis = pool.getResource()) {
            Map.Entry<StreamEntryID, List<StreamEntry>> claimed;
            StreamEntryID cursor = StreamEntryID.MINIMUM_ID;
            do {
                claimed = jedis.xautoclaim(config.getTransactionStream(), config.getConsumerGroup(),
                        consumer, CLAIM_MIN_IDLE_MS, cursor,
                        XAutoClaimParams.xAutoClaimParams().count(100));
                if (claimed == null || claimed.getValue() == null) {
                    return;
                }
                synchronized (ctx.getCheckpointLock()) {
                    for (StreamEntry entry : claimed.getValue()) {
                        emitOrDiscard(ctx, jedis, entry);
                    }
                }
                cursor = claimed.getKey();
            } while (running && cursor != null && !"0-0".equals(cursor.toString()));
        } catch (Throwable e) {
            if (running) {
                log.warn("XAUTOCLAIM 失败", e);
            }
        }
    }

    /** 阻塞读取从未投递给本组的新消息。 */
    private void readNew(SourceContext<StreamTransaction> ctx, String consumer) throws InterruptedException {
        List<Map.Entry<String, List<StreamEntry>>> batches;
        try (Jedis jedis = pool.getResource()) {
            // ">" 只取新消息；BLOCK 5s 保证 cancel 最迟 5 秒响应
            batches = jedis.xreadGroup(config.getConsumerGroup(), consumer,
                    XReadGroupParams.xReadGroupParams().count(100).block(5_000),
                    Map.of(config.getTransactionStream(), StreamEntryID.UNRECEIVED_ENTRY));
        } catch (Exception e) {
            if (!running) {
                return;
            }
            log.warn("XREADGROUP 失败，2s 后重试: {}", e.getMessage());
            Thread.sleep(2_000);
            return;
        }
        if (batches == null || batches.isEmpty()) {
            return;
        }
        synchronized (ctx.getCheckpointLock()) {
            try (Jedis jedis = pool.getResource()) {
                for (var batch : batches) {
                    for (StreamEntry entry : batch.getValue()) {
                        if (!running) {
                            return;
                        }
                        emitOrDiscard(ctx, jedis, entry);
                    }
                }
            }
        }
    }

    /** 解析并下发；毒消息在此直接 ACK 丢弃（正常消息由决策 Sink ACK）。 */
    private void emitOrDiscard(SourceContext<StreamTransaction> ctx, Jedis jedis, StreamEntry entry) {
        String json = entry.getFields().get("json");
        TransactionEvent event;
        try {
            if (json == null) {
                throw new IllegalArgumentException("缺少 json 字段");
            }
            event = JsonCodecs.eventFromJson(json);
        } catch (RuntimeException parseEx) {
            log.error("丢弃无法解析的交易消息 {}: {}", entry.getID(), parseEx.getMessage());
            jedis.xack(config.getTransactionStream(), config.getConsumerGroup(), entry.getID());
            return;
        }
        ctx.collect(new StreamTransaction(entry.getID(), event));
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }
}
