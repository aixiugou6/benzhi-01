package com.risk.engine;

import com.risk.model.Decision;
import com.risk.model.DecisionAction;
import com.risk.model.ReasonCode;
import com.risk.model.TransactionEvent;
import com.risk.rule.RuleSetHolder;
import com.risk.rule.WindowReferences;
import com.risk.rule.expr.EvalException;
import com.risk.rule.expr.Expr;
import com.risk.rule.expr.Expr.EvalContext;
import com.risk.rule.expr.WindowStats;
import com.risk.rule.model.CompiledRule;
import com.risk.rule.model.CompiledRuleSet;
import com.risk.rule.model.WindowDef;
import com.risk.window.GroupKeyResolver;
import com.risk.window.RedisWindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 风控决策引擎（无状态，可被多线程并发调用）。
 *
 * <p>单笔交易处理流程：
 * <ol>
 *   <li>事件合法性校验，不合法直接 {@link DecisionAction#INVALID}；</li>
 *   <li>取当前规则集快照（热更新对进行中的求值无影响）；</li>
 *   <li>对规则集声明的每个滑动窗口做一次 Redis 原子记账，得到 count/sum（含本次）；</li>
 *   <li>规则按优先级顺序求值，<b>第一条命中即拦截</b>并输出该规则原因码；全部不命中则放行；</li>
 *   <li>任何求值/存储异常 fail-closed：输出 BLOCK + SYS_ENGINE_ERROR，宁错拦不漏放。</li>
 * </ol>
 */
public class RiskEngine {

    private static final Logger log = LoggerFactory.getLogger(RiskEngine.class);

    private final RuleSetHolder ruleSetHolder;
    private final RedisWindowStore windowStore;
    private final Clock clock;

    public RiskEngine(RuleSetHolder ruleSetHolder, RedisWindowStore windowStore, Clock clock) {
        this.ruleSetHolder = ruleSetHolder;
        this.windowStore = windowStore;
        this.clock = clock;
    }

    /**
     * 对一笔交易做风控决策。
     *
     * @param event 交易事件（不为 null）
     * @return 决策结果（PASS / BLOCK / INVALID）
     */
    public Decision evaluate(TransactionEvent event) {
        long nowMs = clock.nowMs();

        String invalid = event.validate();
        if (invalid != null) {
            log.warn("非法事件: eventId={}, 原因={}", event.getEventId(), invalid);
            return Decision.invalid(event, invalid, nowMs);
        }

        CompiledRuleSet ruleSet = ruleSetHolder.get();
        if (ruleSet == null) {
            log.error("尚无可用规则集，fail-closed 拦截: eventId={}", event.getEventId());
            return engineError(event, null, nowMs, "规则集尚未加载");
        }

        try {
            Map<String, WindowStats> stats = recordWindows(ruleSet, event, nowMs);
            EvalContext ctx = new EvalContext(event, stats);

            for (CompiledRule rule : ruleSet.rules()) {
                boolean hit;
                try {
                    hit = rule.condition().evalBoolean(ctx);
                } catch (EvalException e) {
                    // 单条规则运行时类型/引用错误：fail-closed，不静默放过
                    log.error("规则 {} 求值异常，fail-closed: eventId={}, err={}",
                            rule.id(), event.getEventId(), e.getMessage());
                    return engineError(event, ruleSet.version(), nowMs,
                            "规则 " + rule.id() + " 求值异常: " + e.getMessage());
                }
                if (hit) {
                    Long windowCount = firstWindowCount(rule, stats);
                    log.info("拦截 eventId={} 规则={} 优先级={} 原因码={} 窗口计数={}",
                            event.getEventId(), rule.id(), rule.priority(),
                            rule.reasonCode(), windowCount);
                    return Decision.block(event.getEventId(), event.getCardNo(),
                            ruleSet.version(), nowMs, rule.id(), rule.priority(),
                            rule.reasonCode(), describe(rule), windowCount);
                }
            }
            return Decision.pass(event.getEventId(), event.getCardNo(), ruleSet.version(), nowMs);
        } catch (RuntimeException e) {
            log.error("引擎处理异常，fail-closed: eventId={}", event.getEventId(), e);
            return engineError(event, ruleSet.version(), nowMs, e.getMessage());
        }
    }

    /** 所有窗口各做一次原子记账，组装成表达式可用的统计上下文。 */
    private Map<String, WindowStats> recordWindows(CompiledRuleSet ruleSet,
                                                   TransactionEvent event, long nowMs) {
        Map<String, WindowStats> stats = new LinkedHashMap<>();
        for (WindowDef window : ruleSet.windows().values()) {
            if (!ruleSet.referencedAliases().contains(window.alias())) {
                continue;
            }
            String groupValue = GroupKeyResolver.resolve(window, event);
            if (groupValue == null) {
                throw new EvalException("窗口 " + window.alias()
                        + " 的分组字段 " + window.keyBy() + " 为空，无法统计");
            }
            var r = windowStore.recordAndCount(window, event, groupValue, nowMs);
            stats.put(window.alias(), new WindowStats(r.count(), r.sumAmount()));
        }
        return stats;
    }

    /** 命中规则的解释文案：优先规则描述，退回原因码枚举描述。 */
    private String describe(CompiledRule rule) {
        if (rule.description() != null && !rule.description().isBlank()) {
            return rule.description();
        }
        return ReasonCode.fromCode(rule.reasonCode()).getDescription();
    }

    /** 取规则条件中第一个窗口别名的计数，填进决策便于下游解释。 */
    private Long firstWindowCount(CompiledRule rule, Map<String, WindowStats> stats) {
        List<String> aliases = WindowReferences.collect(rule.condition());
        if (aliases.isEmpty()) {
            return null;
        }
        WindowStats s = stats.get(aliases.get(0));
        return s == null ? null : s.count();
    }

    private Decision engineError(TransactionEvent event, String version, long nowMs, String detail) {
        return new Decision(DecisionAction.BLOCK, ReasonCode.SYS_ENGINE_ERROR.name(),
                ReasonCode.SYS_ENGINE_ERROR.getDescription() + ": " + detail,
                null, null, null, event.getEventId(), event.getCardNo(), version, nowMs);
    }
}
