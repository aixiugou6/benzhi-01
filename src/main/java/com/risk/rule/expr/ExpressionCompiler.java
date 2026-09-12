package com.risk.rule.expr;

/**
 * 表达式编译器对外门面：字符串条件 → AST。
 *
 * <p>编译只在规则加载/热更新时发生一次；运行时 {@link Expr#eval} 直接走 AST，无解析开销。
 */
public final class ExpressionCompiler {

    private ExpressionCompiler() {
    }

    /**
     * 编译条件表达式。
     *
     * @param text 表达式文本，如 {@code "count(w1) >= 5 AND $amount > 10000"}
     * @return 可重复求值的 AST 根节点
     * @throws RuleParseException 词法或语法错误（信息带位置）
     */
    public static Expr compile(String text) {
        return Parser.parse(text);
    }
}
