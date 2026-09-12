package com.risk.rule.expr;

/**
 * 表达式求值期异常（区别于编译期的 {@link RuleParseException}）。
 */
public class EvalException extends RuntimeException {
    public EvalException(String message) {
        super(message);
    }
}
