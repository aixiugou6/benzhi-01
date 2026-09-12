package com.risk.model;

/**
 * 风控动作（决策结论）。
 * <ul>
 *     <li>{@link #PASS}：通过，交易链路继续；</li>
 *     <li>{@link #BLOCK}：拦截，交易链路据此拒绝交易；</li>
 *     <li>{@link #INVALID}：事件本身不合法（缺字段/金额为负），无法参与规则求值。</li>
 * </ul>
 */
public enum DecisionAction {
    PASS,
    BLOCK,
    INVALID
}
