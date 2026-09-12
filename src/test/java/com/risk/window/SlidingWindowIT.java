package com.risk.window;

import com.risk.RedisIntegrationTest;
import com.risk.model.TransactionEvent;
import com.risk.rule.model.WindowDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 模块二：基于 Redis Lua 的精确滑动窗口统计（真实 Redis）。
 */
class SlidingWindowIT extends RedisIntegrationTest {

    private final WindowDef w1 = new WindowDef("w1", "cardNo", 1_000L);
    private RedisWindowStore store;

    @BeforeEach
    void setUp() {
        flushDb();
        store = new RedisWindowStore(pool);
    }

    private TransactionEvent txn(String id, String card, double amount) {
        return new TransactionEvent(id, card, amount, "CNY", "M-1", "PURCHASE", null);
    }

    @Test
    void 次数与金额随每笔交易累计() {
        long t = 10_000L;
        var r1 = store.recordAndCount(w1, txn("e1", "CARD-A", 100), "CARD-A", t);
        var r2 = store.recordAndCount(w1, txn("e2", "CARD-A", 200.5), "CARD-A", t + 100);
        var r3 = store.recordAndCount(w1, txn("e3", "CARD-A", 50), "CARD-A", t + 200);

        assertEquals(1, r1.count());
        assertTrue(r1.newlyAdded());
        assertEquals(2, r2.count());
        assertEquals(3, r3.count());
        assertEquals(350.5, r3.sumAmount(), 1e-9);
    }

    @Test
    void 窗口边界_恰好落在窗口外的旧事件被剔除() {
        store.recordAndCount(w1, txn("e0", "CARD-A", 100), "CARD-A", 10_000L);
        store.recordAndCount(w1, txn("e1", "CARD-A", 100), "CARD-A", 10_500L);
        // now=11000，cutoff=10000：score<=10000 的 e0 恰好滑出，窗口只剩 e1 与本次
        var r = store.recordAndCount(w1, txn("e2", "CARD-A", 100), "CARD-A", 11_000L);
        assertEquals(2, r.count());
        assertEquals(200, r.sumAmount(), 1e-9);
    }

    @Test
    void 窗口边界_窗口内最后一毫秒仍保留() {
        store.recordAndCount(w1, txn("e0", "CARD-A", 100), "CARD-A", 10_000L);
        // now=10999，cutoff=9999：e0 仍在窗口内
        var r = store.recordAndCount(w1, txn("e1", "CARD-A", 100), "CARD-A", 10_999L);
        assertEquals(2, r.count());
    }

    @Test
    void 同一eventId重复投递幂等不重复计数() {
        var first = store.recordAndCount(w1, txn("dup", "CARD-A", 100), "CARD-A", 10_000L);
        var again = store.recordAndCount(w1, txn("dup", "CARD-A", 999), "CARD-A", 10_100L);
        assertTrue(first.newlyAdded());
        assertFalse(again.newlyAdded());
        assertEquals(1, again.count());
        // 金额以第一次为准，不被重复投递改写
        assertEquals(100, again.sumAmount(), 1e-9);
    }

    @Test
    void 不同卡号互不影响() {
        store.recordAndCount(w1, txn("a1", "CARD-A", 100), "CARD-A", 10_000L);
        store.recordAndCount(w1, txn("a2", "CARD-A", 100), "CARD-A", 10_100L);
        var onB = store.recordAndCount(w1, txn("b1", "CARD-B", 100), "CARD-B", 10_200L);
        assertEquals(1, onB.count());
    }

    @Test
    void key设置了过期时间_冷数据自动回收() {
        store.recordAndCount(w1, txn("e1", "CARD-A", 100), "CARD-A", 10_000L);
        try (Jedis jedis = pool.getResource()) {
            String zkey = RedisWindowStore.zsetKey("w1", "CARD-A");
            long ttl = jedis.ttl(zkey);
            assertTrue(ttl > 0 && ttl <= (w1.windowMs() + 60_000L) / 1000,
                    "TTL 应在 (0, 窗口+宽限] 内，实际=" + ttl);
        }
    }

    @Test
    void 不传时间时使用Redis节点时间() {
        var r = store.recordAndCount(w1, txn("e1", "CARD-A", 100), "CARD-A", null);
        assertTrue(r.usedNowMs() > 1_700_000_000_000L, "应返回 Redis TIME 派生的毫秒时间戳");
        assertEquals(1, r.count());
    }

    @Test
    void EVALSHA脚本丢失后自动重载() {
        // 先正常调用完成 SCRIPT LOAD
        store.recordAndCount(w1, txn("e1", "CARD-A", 100), "CARD-A", 10_000L);
        try (Jedis jedis = pool.getResource()) {
            jedis.scriptFlush();
        }
        // 模拟 Redis 重启/脚本被清：NOSCRIPT 后应自愈
        var r = store.recordAndCount(w1, txn("e2", "CARD-A", 100), "CARD-A", 10_100L);
        assertEquals(2, r.count());
    }
}
