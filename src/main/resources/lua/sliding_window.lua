-- 滑动窗口记账与统计（在 Redis 单线程内原子执行）
--
-- KEYS[1] = ZSET，成员 eventId、score=事件进入 Redis 的时间(ms)
-- KEYS[2] = HASH，eventId -> 金额（金额与成员同生命周期）
-- ARGV[1] = eventId（幂等键：重复投递不重复计数）
-- ARGV[2] = amount（本次金额）
-- ARGV[3] = windowMs（窗口长度，毫秒）
-- ARGV[4] = nowMs（客户端注入时间；<0 时用 Redis 节点时间，生产默认走节点时间）
-- ARGV[5] = ttlMs（key 过期时间，窗口长度 + 宽限）
--
-- 返回：{ 窗口内笔数(含本次), 窗口内金额合计(含本次), 本次是否新写入(1/0), 实际使用的时间戳ms }

local now = tonumber(ARGV[4])
if now < 0 then
  local t = redis.call('TIME')
  now = t[1] * 1000 + math.floor(t[2] / 1000)
end

local window = tonumber(ARGV[3])
local cutoff = now - window

-- 1) 清理滑出窗口的成员（连同金额 HASH 一起删，避免泄漏）
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', cutoff)
if #expired > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
  redis.call('HDEL', KEYS[2], unpack(expired))
end

-- 2) 记入本次交易；NX 保证同一 eventId 重复投递只计一次
local added = redis.call('ZADD', KEYS[1], 'NX', now, ARGV[1])
if added == 1 then
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
end

-- 3) 统计窗口内笔数与金额合计
local count = redis.call('ZCARD', KEYS[1])
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
local sum = 0.0
if #members > 0 then
  local vals = redis.call('HMGET', KEYS[2], unpack(members))
  for i = 1, #vals do
    if vals[i] then
      sum = sum + tonumber(vals[i])
    end
  end
end

-- 4) 续期，冷 key 自动回收
local ttl = tonumber(ARGV[5])
redis.call('PEXPIRE', KEYS[1], ttl)
redis.call('PEXPIRE', KEYS[2], ttl)

return { count, tostring(sum), added, tostring(now) }
