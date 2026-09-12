package com.risk.rule;

import com.risk.rule.model.CompiledRuleSet;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 规则集持有者：{@code volatile} 语义的原子引用，热更新时整体替换。
 *
 * <p>读路径（每个交易事件）无锁：拿到某一版 {@link CompiledRuleSet} 后整次求值都用该版，
 * 即使求值期间发生热更新也不会出现“半新半旧”；写路径只在规则文件变更时触发一次。
 */
public class RuleSetHolder {

    private final AtomicReference<CompiledRuleSet> current = new AtomicReference<>();

    /** @return 当前生效规则集；尚未加载任何规则时返回 null */
    public CompiledRuleSet get() {
        return current.get();
    }

    /** 原子替换为新编译的规则集。 */
    public void set(CompiledRuleSet ruleSet) {
        current.set(ruleSet);
    }

    public String version() {
        CompiledRuleSet rs = current.get();
        return rs == null ? null : rs.version();
    }
}
