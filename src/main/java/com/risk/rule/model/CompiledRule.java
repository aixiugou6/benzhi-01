package com.risk.rule.model;

import com.risk.rule.expr.Expr;

/**
 * 编译后的规则：AST + 元数据，运行时直接求值。
 */
public record CompiledRule(
        String id,
        int priority,
        String reasonCode,
        String description,
        Expr condition) {

    /** 从原始规则构造。 */
    public static CompiledRule of(Rule r, Expr condition) {
        return new CompiledRule(r.getId(), r.getPriority(), r.getReasonCode(),
                r.getDescription(), condition);
    }
}
