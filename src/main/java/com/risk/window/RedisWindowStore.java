package com.risk.window;

import com.risk.model.TransactionEvent;
import com.risk.rule.model.WindowDef;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * 基于 Redis 的精确滑动窗口存储。
 *
 * <p>每个 {@code (窗口别名, 分组值) } 对应一个 ZSET（成员 eventId、score=毫秒时间戳）
 * 加一个 HASH（eventId→金额），记账与统计在单个 Lua 脚本中原子完成：
 * 清理过期成员 → NX 幂等写入 → ZCARD/HMGET 统计 → 续期 TTL。
 *
 * <p>时间默认取 Redis 节点 {@code TIME}（多客户端时钟不影响窗口正确性）；
 * 也可显式注入 nowMs（测试用来精确验证边界）。
 */
public class RedisWindowStore implements AutoCloseable {

    private static final String KEY_PREFIX = "risk:w:";
    private static final long TTL_GRACE_MS = 60_000L;

    private final JedisPool pool;
    private final String script;
    private volatile String sha;

    /**
     * @param pool Jedis 连接池（生命周期由调用方或本类 close 管理）
     */
    public RedisWindowStore(JedisPool pool) {
        this.pool = pool;
        this.script = loadScript();
    }

    /**
     * 原子地把当前交易记入窗口并返回窗口统计（含本次）。
     *
     * @param window     窗口定义
     * @param event      当前交易（取 eventId/金额）
     * @param groupValue 分组键值（如卡号；null 由调用方提前拦截）
     * @param nowMs      记账时间；null 表示使用 Redis 节点时间
     */
    public WindowResult recordAndCount(WindowDef window, TransactionEvent event,
                                       String groupValue, Long nowMs) {
        String zkey = zsetKey(window.alias(), groupValue);
        String hkey = zkey + ":amt";
        long ttl = window.windowMs() + TTL_GRACE_MS;
        List<String> argv = List.of(
                event.getEventId(),
                Double.toString(event.getAmount()),
                Long.toString(window.windowMs()),
                Long.toString(nowMs == null ? -1L : nowMs),
                Long.toString(ttl));

        Object raw;
        try (Jedis jedis = pool.getResource()) {
            try {
                raw = jedis.evalsha(ensureSha(jedis), List.of(zkey, hkey), argv);
            } catch (JedisNoScriptException e) {
                // Redis 重启/被 SCRIPT FLUSH 后自动重新加载脚本
                this.sha = jedis.scriptLoad(script);
                raw = jedis.evalsha(sha, List.of(zkey, hkey), argv);
            }
        }

        // 注意：Lua 多元素嵌套数组经 Jedis 解出后元素均为 String（即使脚本内是整数），
        // 这里统一按字符串解析，不能强转 Number。
        List<?> res = (List<?>) raw;
        long count = Long.parseLong(res.get(0).toString());
        double sum = Double.parseDouble(res.get(1).toString());
        boolean added = Long.parseLong(res.get(2).toString()) == 1L;
        long usedNow = Long.parseLong(res.get(3).toString());
        return new WindowResult(count, sum, added, usedNow);
    }

    /** 删除某窗口/分组的全部数据（测试隔离用）。 */
    public void deleteWindow(WindowDef window, String groupValue) {
        try (Jedis jedis = pool.getResource()) {
            String zkey = zsetKey(window.alias(), groupValue);
            jedis.del(zkey, zkey + ":amt");
        }
    }

    private String ensureSha(Jedis jedis) {
        String s = sha;
        if (s == null) {
            synchronized (this) {
                if (sha == null) {
                    sha = jedis.scriptLoad(script);
                }
                s = sha;
            }
        }
        return s;
    }

    /**
     * key 形如 {@code risk:w:w1:9f2a..（分组值 SHA-256）}。
     * 对分组值做哈希，避免卡号直接落 key，也杜绝分隔符碰撞。
     */
    static String zsetKey(String alias, String groupValue) {
        return KEY_PREFIX + alias + ":" + sha256(groupValue);
    }

    private static String sha256(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }

    private static String loadScript() {
        try (InputStream in = RedisWindowStore.class.getClassLoader()
                .getResourceAsStream("lua/sliding_window.lua")) {
            if (in == null) {
                throw new IllegalStateException("找不到 lua/sliding_window.lua");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("加载滑动窗口 Lua 脚本失败", e);
        }
    }

    @Override
    public void close() {
        // JedisPool 由应用统一管理生命周期；这里不关闭
    }
}
