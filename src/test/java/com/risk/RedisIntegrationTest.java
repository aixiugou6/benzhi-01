package com.risk;

import com.risk.config.RiskEngineConfig;
import com.risk.redis.JedisFactory;
import org.junit.jupiter.api.BeforeAll;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 集成测试基类：连接本机 Redis（默认 127.0.0.1:6379，
 * 可用环境变量 RISK_REDIS_HOST/RISK_REDIS_PORT 覆盖）。
 * Redis 不可达时测试中止（abort），而不是误报失败。
 */
public abstract class RedisIntegrationTest {

    protected static RiskEngineConfig config;
    protected static JedisPool pool;

    @BeforeAll
    static void connectRedis() {
        config = new RiskEngineConfig();
        String host = System.getenv().getOrDefault("RISK_REDIS_HOST", "127.0.0.1");
        String portEnv = System.getenv("RISK_REDIS_PORT");
        if (portEnv != null && !portEnv.isBlank()) {
            config.setRedisPort(Integer.parseInt(portEnv));
        }
        config.setRedisHost(host);

        boolean reachable;
        try (Jedis jedis = new Jedis(host, config.getRedisPort(), 2_000)) {
            jedis.ping();
            reachable = true;
        } catch (Exception e) {
            reachable = false;
        }
        assumeTrue(reachable, "Redis 不可达，跳过重依赖集成测试（请先按 README 启动 Redis）");
        pool = JedisFactory.create(config);
    }

    /** 每个测试前清空当前 DB，保证互不干扰。 */
    protected void flushDb() {
        try (Jedis jedis = pool.getResource()) {
            jedis.flushDB();
        }
    }
}
