# 实时风控引擎（Realtime Risk Engine）

用户点击支付 → 交易事件进入事件流 → 引擎按规则实时计算（滑动窗口次数/累计金额、单笔金额等）
→ 输出 **`PASS`（通过）/ `BLOCK`（拦截）+ 原因码** → 交易链路据此放行或拒绝。
规则写在 JSON 文件里，**修改规则无需重启服务**（默认 5 秒内热生效，坏规则整版拒绝、线上继续跑旧版）。

技术栈（均写死在 `pom.xml`，未使用其它替代框架）：

| 职责 | 选型 |
|---|---|
| 语言/构建 | Java 17、Maven |
| 流式处理 | Apache Flink 1.18（DataStream，`java -jar` 内嵌 MiniCluster 常驻运行，无需独立集群） |
| 状态/统计/事件流 | Redis 7（Streams 做事件总线，ZSET+HASH+Lua 做精确滑动窗口） |
| Redis 客户端 | Jedis 5.1 |
| JSON | Jackson 2.17 |
| 测试 | JUnit 5 + Awaitility（真实 Redis 集成测试 + Flink MiniCluster 端到端测试） |

---

## 1. 架构与处理链路

```
支付链路 ──XADD json──► Redis Stream: risk.transactions
                                   │
                          Flink source（消费者组, at-least-once）
                                   │ keyBy(cardNo)
        规则文件 config/rules.json ─┼─► KeyedBroadcastProcessFunction（风控核心算子）
        （轮询热更新 → 广播原文）    │        │
                                   │        ├─ 规则文件 → RuleCompiler：词法/语法 → AST → 静态类型检查
                                   │        ├─ 每个被引用窗口：Redis Lua 原子记账（清理过期 → NX 幂等写入 → count/sum → TTL）
                                   │        └─ 规则按优先级顺序求值，第一条命中即 BLOCK
                                   ▼
                          Redis Stream: risk.decisions （决策 JSON，写成功后再 XACK 交易条目）
                                   │
支付链路 ◄──读取决策（PASS/BLOCK + reasonCode）──┘
```

**关键语义**

- **精确滑动窗口**：不是近似计数器。每笔交易是 ZSET 中一个带毫秒时间戳的成员，
  统计前先剔除 `score <= now-windowMs` 的成员，窗口边界精确到毫秒。
- **幂等**：ZSET 成员为 `eventId` 且 `ZADD NX`，同一条交易重复投递不会重复计数。
- **时间基准**：窗口记账时间默认取 **Redis 节点 `TIME`**（多实例时钟不一致也不影响正确性）；
  金额与成员同生命周期，key 设置 TTL 自动回收。
- **优先级**：`priority` 数字越小越先判；同优先级按规则 id 字典序，求值顺序确定。
  多条同时满足时，**只输出第一条命中规则的原因码**。
- **fail-closed**：规则未加载 / 求值或存储异常 → 输出 `BLOCK + SYS_ENGINE_ERROR`，宁错拦不漏放；
  只有输入事件本身缺字段/金额为负才输出 `INVALID`。
- **热更新安全**：规则先编译（JSON + 语法 + 类型 + 引用全部校验）通过才替换；
  编译失败的文件被整版拒绝，线上保留旧规则。求值期间热更新不影响进行中的决策（快照读）。

---

## 2. 目录结构

```
.
├── pom.xml
├── config/
│   ├── application.json      # 运行配置（Redis 地址、流名、规则路径、热更新间隔…）
│   └── rules.json            # 规则文件（热更新对象，改这个文件即可）
├── src/main/java/com/risk/
│   ├── RiskEngineApplication.java   # 常驻服务入口 + Flink 拓扑装配
│   ├── config/RiskEngineConfig.java
│   ├── model/                       # 交易事件、决策 Decision、动作、原因码
│   ├── rule/
│   │   ├── RuleCompiler.java        # 规则模型编译：解析+静态类型检查+优先级排序
│   │   ├── RuleSetHolder.java       # 规则集原子引用（copy-on-write）
│   │   ├── WindowReferences.java
│   │   ├── model/                   # Rule/RuleSet/WindowDef/Compiled*
│   │   └── expr/                    # 规则条件 DSL：Lexer/Parser/AST/求值
│   ├── window/RedisWindowStore.java # Lua 原子滑动窗口（EVALSHA + NOSCRIPT 自愈）
│   ├── engine/RiskEngine.java       # 规则求值、优先级、决策与原因码、fail-closed
│   ├── stream/                      # Flink source/算子/sink、广播热更新
│   └── redis/JedisFactory.java
├── src/main/resources/lua/sliding_window.lua
└── src/test/                        # 5 大模块的单元/集成/端到端测试
```

---

## 3. 环境准备（二选一）

### 方式 A：Docker 官方镜像（推荐）

需要 Docker。用 Redis **官方镜像** 起一个 Redis 7：

```bash
docker run -d --name risk-redis -p 6379:6379 redis:7
docker exec risk-redis redis-cli ping    # 输出 PONG
```

停止与清理：

```bash
docker rm -f risk-redis
```

### 方式 B：官方发行包（无 Docker 时）

- **macOS**：`brew install redis`，然后 `redis-server --daemonize yes`
- **Debian/Ubuntu**：`sudo apt-get update && sudo apt-get install -y redis-server`
  （官方仓库的 redis-server，默认已在 6379 启动）
- **Windows**：用 WSL 执行上面的 Debian 步骤，或用方式 A 的 Docker 镜像

验证：`redis-cli ping` 输出 `PONG`。

### JDK 与 Maven

- JDK 17（Eclipse Temurin / OpenJDK 均可）：`java -version` 能看到 17
- Maven 3.8+：`mvn -version`

---

## 4. 构建

```bash
mvn -q package
```

产物：`target/risk-engine.jar`（可执行 fat jar）。

---

## 5. 运行常驻服务

确保 Redis 已在 `127.0.0.1:6379`，然后：

```bash
java -jar target/risk-engine.jar                 # 默认读 config/application.json
# 或显式指定配置：
java -jar target/risk-engine.jar /path/to/application.json
```

看到类似日志即就绪：

```
... 消费组 risk-engine 创建于 stream risk.transactions
... 初始规则集已加载: version=rules-001, 规则数=4, 窗口数=2
```

配置全部可用环境变量覆盖，例如：

```bash
RISK_REDIS_HOST=127.0.0.1 RISK_REDIS_PORT=6379 \
RISK_RULES_FILE=./config/rules.json RISK_RULES_RELOAD_SECONDS=5 \
java -jar target/risk-engine.jar
```

| 配置项 | 环境变量 | 默认 | 说明 |
|---|---|---|---|
| redisHost / redisPort | `RISK_REDIS_HOST` / `RISK_REDIS_PORT` | 127.0.0.1 / 6379 | Redis 地址 |
| redisPassword | `RISK_REDIS_PASSWORD` | 无 | Redis 密码 |
| redisDatabase | `RISK_REDIS_DB` | 0 | 库序号 |
| transactionStream | `RISK_TXN_STREAM` | risk.transactions | 交易输入流 |
| decisionStream | `RISK_DECISION_STREAM` | risk.decisions | 决策输出流 |
| consumerGroup | `RISK_CONSUMER_GROUP` | risk-engine | 消费组 |
| rulesFile | `RISK_RULES_FILE` | config/rules.json | 规则文件路径 |
| rulesReloadSeconds | `RISK_RULES_RELOAD_SECONDS` | 5 | 规则轮询间隔 |
| parallelism | `RISK_PARALLELISM` | 1 | Flink 并行度 |

---

## 6. 发一笔交易、看决策结果

另开两个终端。

**发送交易事件**（字段：eventId 唯一、cardNo、amount、currency、merchantId、txnType、eventTimeMs 可省略）：

```bash
redis-cli XADD risk.transactions '*' json \
'{"eventId":"E-1001","cardNo":"CARD-001","amount":120.00,"currency":"CNY","merchantId":"M-7","txnType":"PURCHASE"}'
```

**读取引擎决策**：

```bash
redis-cli XREVRANGE risk.decisions + - COUNT 1
```

返回的 JSON：

```json
{"action":"PASS","reasonCode":"NONE","reason":"无风险，放行",
 "eventId":"E-1001","cardNo":"CARD-001","ruleSetVersion":"rules-001","decisionTimeMs":1726000000123}
```

用内置规则验证「同一张卡 1 分钟刷 5 次就拦」（连刷同一卡号 5 笔）：

```bash
for i in 1 2 3 4 5; do
  redis-cli XADD risk.transactions '*' json \
  "{\"eventId\":\"HF-$i\",\"cardNo\":\"CARD-888\",\"amount\":99.00,\"currency\":\"CNY\",\"merchantId\":\"M-1\",\"txnType\":\"PURCHASE\"}"
  sleep 0.3
done
redis-cli XREVRANGE risk.decisions + - COUNT 1
```

第 5 笔输出（正是需求里的「拦截，原因：短时高频」）：

```json
{"action":"BLOCK","reasonCode":"R_HIGH_FREQUENCY","reason":"拦截，原因：短时高频（同卡 1 分钟内 >=5 笔）",
 "matchedRuleId":"high_freq_1m_5","priority":10,"windowCount":5,
 "eventId":"HF-5","cardNo":"CARD-888","ruleSetVersion":"rules-001","decisionTimeMs":...}
```

---

## 7. 规则文件与热更新

规则文件是 `config/rules.json`，顶层结构：

```json
{
  "version": "rules-001",
  "windows": [
    { "alias": "w1", "keyBy": "cardNo", "windowMs": 60000 },
    { "alias": "w2", "keyBy": "cardNo", "windowMs": 3600000 }
  ],
  "rules": [
    {
      "id": "high_freq_1m_5",
      "enabled": true,
      "priority": 10,
      "reasonCode": "R_HIGH_FREQUENCY",
      "description": "拦截，原因：短时高频（同卡 1 分钟内 >=5 笔）",
      "condition": "count(w1) >= 5"
    }
  ]
}
```

- `windows[].keyBy` 支持 `cardNo / merchantId / txnType / currency`；
  `windowMs` 是窗口长度。
- `priority` 越小越先判，命中第一条即拦截。`enabled:false` 可临时停用规则。
- 每次修改把 `version` 改一下，决策里会带上当时的规则版本，便于回溯。

### 条件表达式语法

| 类别 | 写法 |
|---|---|
| 事件字段 | `$amount` `$cardNo` `$currency` `$merchantId` `$txnType` `$eventId` |
| 窗口函数 | `count(w1)`（窗口内笔数，含本次）、`sum(w2)`（窗口内金额合计，含本次） |
| 比较 | `>  >=  <  <=  ==  !=`（数值比数值，字符串比字符串，字符串用单/双引号） |
| 布尔 | `AND` `OR` `NOT` 与括号；关键字大小写不敏感 |

示例：

```
count(w1) >= 5
$amount >= 50000
$txnType == 'WITHDRAW' AND count(w1) >= 3
sum(w2) >= 100000
($txnType == 'WITHDRAW' OR $amount > 40000) AND count(w1) < 5
```

### 热更新演示（服务不重启）

把阈值从 5 改成 2：

```bash
sed -i 's/"count(w1) >= 5"/"count(w1) >= 2"/' config/rules.json
```

约 5 秒内日志出现 `热更新规则集已加载: version=...`，之后同卡第 2 笔即被拦截。

写入一段非法 JSON 或不合法规则，日志会报
`规则文件存在 N 处错误 …` / `收到非法规则文件，拒绝热更新`，**线上继续用旧规则**；
把文件改正确后会自动重新加载。

---

## 8. 决策结果与原因码

决策 JSON 字段：

| 字段 | 含义 |
|---|---|
| `action` | `PASS` / `BLOCK` / `INVALID` |
| `reasonCode` | 原因码（见下），交易链路据此做差异化处理 |
| `reason` | 人可读原因 |
| `matchedRuleId` / `priority` | 命中的规则及其优先级（PASS 时为 null） |
| `windowCount` | 命中规则的窗口计数（含本次，无窗口统计时为 null） |
| `eventId` / `cardNo` | 对应交易 |
| `ruleSetVersion` | 做出该决策时的规则版本 |
| `decisionTimeMs` | 决策时间戳 |

原因码：

| 码 | 含义 |
|---|---|
| `NONE` | 放行 |
| `R_HIGH_FREQUENCY` | 短时高频（次数达阈值） |
| `R_AMOUNT_EXCEEDED` | 单笔金额异常 |
| `R_WINDOW_AMOUNT_EXCEEDED` | 窗口期累计金额异常 |
| 规则中自定义的任意码 | 原样输出（如 `R_BLACKLIST`） |
| `SYS_INVALID_EVENT` | 输入事件不合法（缺字段/负金额） |
| `SYS_ENGINE_ERROR` | 引擎内部错误，fail-closed 拦截 |

---

## 9. 测试

测试分 5 块，其中窗口/引擎/端到端为**真实 Redis 集成测试**（非内嵌、非 mock），
端到端测试会真实启动 Flink MiniCluster 跑线上同一张拓扑。

```bash
# 先确保 Redis 在 127.0.0.1:6379（可用 RISK_REDIS_HOST/RISK_REDIS_PORT 指向其它实例）
mvn test
```

> Redis 不可达时，重依赖集成测试会 **skip**（JUnit assumption），纯单元测试仍会运行。

| # | 测试类 | 覆盖模块 |
|---|---|---|
| 1 | `RuleCompilerTest` / `ExpressionEvalTest` | 规则模型与编译、表达式 DSL（词法/语法/AST/静态类型/求值） |
| 2 | `SlidingWindowIT` | 滑动窗口统计（真实 Redis Lua） |
| 3 | `RiskEngineIT` | 规则求值、优先级、决策与原因码、fail-closed |
| 4 | （并入 3 的决策断言） | 决策结果与原因码（含版本、windowCount、INVALID） |
| 5 | `HotReloadEndToEndIT` | 常驻拓扑 + 事件流收发 + 规则热更新/坏规则拒绝/毒消息 |

测试用到的 Redis key 均在独立 DB（默认 db0）内 `FLUSHDB`，请勿与重要数据共用同一库；
也可 `RISK_REDIS_DB=15` 指向测试专用库。

---

## 10. 已验证的边界条件

- **窗口边界到毫秒**：`score == now-windowMs` 的事件恰好滑出（`<=cutoff` 剔除）；
  窗口内最后一毫秒的事件仍保留（`SlidingWindowIT` 两个边界用例）。
- **幂等/重复投递**：同一 `eventId` 重放，`ZADD NX` 不重复计数、金额以首次为准。
- **优先级确定性**：金额规则（priority=5）先于高频规则（priority=10）命中；
  同优先级按 id 字典序。
- **第 5 笔命中、第 4 笔放行**；超过窗口长度后计数滑出重新放行；不同卡号窗口隔离。
- **热更新不重启**：运行中 v1→v2→坏文件（拒绝并保留 v2）→v3，决策版本号随之变化。
- **毒消息**：无法解析的交易消息被 ACK 丢弃，不阻塞后续正常交易。
- **ACK 语义**：决策写入成功后才 XACK，PEL 归零；未 ACK 的消息由 XAUTOCLAIM 在
  空闲 30s 后重领处理（配合 eventId 幂等不重复计数）。
- **NOSCRIPT 自愈**：`SCRIPT FLUSH`/Redis 重启后首次 EVALSHA 失败会自动重新加载 Lua。
- **冷 key 回收**：ZSET/HASH 都设置 `窗口+60s` 的 TTL。
- **fail-closed**：规则未加载、分组字段为空导致无法统计、求值异常 → BLOCK+SYS_ENGINE_ERROR；
  负金额/缺 eventId/缺 cardNo → INVALID。
- **卡号不进 key**：Redis key 对分组值做 SHA-256，避免卡号明文落 key 与分隔符碰撞。

## 11. 已知限制 / 不确定点（如实说明）

- **处理语义是 at-least-once**：崩溃重投可能产生重复决策行，下游应按 `eventId` 去重；
  窗口计数本身因 eventId 幂等不会重复。未接入 Flink checkpoint 的 exactly-once
  （Redis Sink 无两阶段提交），生产若需强一致需额外做事务/幂等表。
- **水平扩展与 keyBy**：交易按 `cardNo` 分区；窗口统计集中在 Redis，扩并行度主要提升吞吐。
  单 Redis 实例是当前瓶颈/单点，生产应换 Redis Sentinel/Cluster 并评估大 key
  （本实现每卡每窗口独立 key，天然分散）。
- **规则 DSL 为有意收敛的子集**（字段 + count/sum + 比较 + 布尔），没有放开任意脚本，
  以保证可静态校验、可测试、无注入风险；如需 `avg/distinct/黑名单集合` 等需要扩展 DSL 与 Lua。
- **热更新检测基于轮询文件 mtime+size**（默认 5s，配置可调），不是 inotify；
  容器内通过挂载卷更新文件时同样有效。要求原子写入（写临时文件再 rename）以避免读到半截文件；
  即便读到半截，编译失败也会拒绝并保留旧版。
- 决策流用 `MAXLEN ~ 1_000_000` 近似裁剪防无界增长；交易流未自动裁剪，由部署方按需设容量。
- 时间基准用 Redis 节点时钟，请确保 Redis 节点使用 NTP；`eventTimeMs` 字段目前用于事件溯源，
  窗口以入 Redis 时间为准（避免客户端时钟作弊/乱序），如需按事件时间可再扩展。
