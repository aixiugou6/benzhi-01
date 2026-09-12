package com.risk.rule.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 一条风控规则（规则文件中的原始声明）。
 *
 * <pre>
 * {
 *   "id": "high_freq_1m_5",
 *   "enabled": true,
 *   "priority": 10,
 *   "reasonCode": "R_HIGH_FREQUENCY",
 *   "description": "同一张卡 1 分钟内交易达到 5 笔则拦截",
 *   "condition": "count(w1) >= 5"
 * }
 * </pre>
 *
 * 字段说明：
 * <ul>
 *   <li>{@code priority}：数字越小优先级越高；同优先级按 id 字典序，保证求值顺序确定；</li>
 *   <li>{@code reasonCode}：命中时输出的原因码（可用系统枚举码，也可自定义字符串）；</li>
 *   <li>{@code condition}：条件表达式，语法见 {@link com.risk.rule.expr.ExpressionCompiler}；</li>
 *   <li>{@code enabled=false}：临时停用时保留规则但不参与求值（热更新常用）。</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Rule {

    private String id;
    private boolean enabled = true;
    private int priority;
    private String reasonCode;
    private String description;
    private String condition;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public void setReasonCode(String reasonCode) {
        this.reasonCode = reasonCode;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
