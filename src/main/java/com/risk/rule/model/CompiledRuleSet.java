package com.risk.rule.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 编译后的规则集：不可变，热更新时整体替换引用（copy-on-write），处理中的事件无锁读旧版本。
 *
 * @param version           版本号
 * @param windows           窗口定义（别名 → 定义）
 * @param rules             已按优先级排序的规则（数字小优先；同优先级按 id 字典序）
 * @param referencedAliases 至少被一条启用规则引用的窗口别名集合（只为这些窗口做 Redis 记账）
 */
public record CompiledRuleSet(
        String version,
        Map<String, WindowDef> windows,
        List<CompiledRule> rules,
        Set<String> referencedAliases) {

    /** 窗口别名集合。 */
    public Set<String> windowAliases() {
        return windows.keySet();
    }
}
