package com.risk.redis;

import com.risk.config.RiskEngineConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Jedis 连接池工厂（全引擎各 Flink 任务共享同一套连接参数）。
 */
public final class JedisFactory {

    private JedisFactory() {
    }

    public static JedisPool create(RiskEngineConfig cfg) {
        JedisPoolConfig poolCfg = new JedisPoolConfig();
        poolCfg.setMaxTotal(16);
        poolCfg.setMaxIdle(8);
        poolCfg.setMinIdle(1);
        poolCfg.setTestOnBorrow(true);

        DefaultJedisClientConfig.Builder clientCfg = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(3_000)
                .socketTimeoutMillis(10_000)
                .database(cfg.getRedisDatabase());
        if (cfg.getRedisPassword() != null && !cfg.getRedisPassword().isBlank()) {
            clientCfg.password(cfg.getRedisPassword());
        }
        return new JedisPool(poolCfg, new HostAndPort(cfg.getRedisHost(), cfg.getRedisPort()),
                clientCfg.build());
    }
}
