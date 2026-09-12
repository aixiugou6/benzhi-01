package com.risk.rule.expr;

/**
 * 规则表达式编译（解析）期异常，携带出错位置便于规则作者定位。
 */
public class RuleParseException extends RuntimeException {
    public RuleParseException(String message) {
        super(message);
    }

    public RuleParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
