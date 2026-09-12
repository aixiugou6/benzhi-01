package com.risk.rule.expr;

import com.risk.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 模块一配套：表达式 DSL 词法/语法/AST/求值。
 */
class ExpressionEvalTest {

    private final TransactionEvent event =
            new TransactionEvent("E1", "CARD-1", 8_000.0, "CNY", "M-9", "WITHDRAW", 1_700_000_000_000L);

    private boolean eval(String expr, Map<String, WindowStats> stats) {
        return ExpressionCompiler.compile(expr).evalBoolean(new Expr.EvalContext(event, stats));
    }

    private boolean eval(String expr) {
        return eval(expr, Map.of());
    }

    @Test
    void 数值字段比较() {
        assertTrue(eval("$amount >= 8000"));
        assertFalse(eval("$amount > 8000"));
        assertTrue(eval("$amount == 8000"));
        assertTrue(eval("$amount != 9"));
    }

    @Test
    void 字符串字段比较() {
        assertTrue(eval("$txnType == 'WITHDRAW'"));
        assertTrue(eval("$currency == \"CNY\""));
        assertTrue(eval("$merchantId != 'M-1'"));
    }

    @Test
    void 窗口count与sum函数() {
        Map<String, WindowStats> stats = Map.of(
                "w1", new WindowStats(5, 40_000.0),
                "w2", new WindowStats(2, 100.0));
        assertTrue(eval("count(w1) >= 5", stats));
        assertFalse(eval("count(w1) > 5", stats));
        assertTrue(eval("sum(w1) >= 40000", stats));
        assertTrue(eval("count(w2) == 2 AND sum(w2) < 200", stats));
    }

    @Test
    void 布尔组合与括号改变优先级() {
        Map<String, WindowStats> stats = Map.of("w1", new WindowStats(5, 1.0));
        // AND 优先于 OR：true OR (false AND false) = true
        assertTrue(eval("$amount == 8000 OR count(w1) < 2 AND $txnType == 'PURCHASE'", stats));
        // 括号：(true OR false) AND false = false
        assertFalse(eval("($amount == 8000 OR count(w1) < 2) AND $txnType == 'PURCHASE'", stats));
        assertTrue(eval("NOT $txnType == 'PURCHASE'", stats));
        assertTrue(eval("NOT (count(w1) < 5)", stats));
    }

    @Test
    void 关键字大小写不敏感() {
        assertTrue(eval("$amount == 8000 and $txnType == 'WITHDRAW'"));
        assertTrue(eval("$amount == 1 or $txnType == 'WITHDRAW'"));
    }

    @Test
    void 语法错误带位置() {
        RuleParseException e1 = assertThrows(RuleParseException.class,
                () -> ExpressionCompiler.compile("count(w1) >="));
        assertTrue(e1.getMessage().contains("位置"), e1.getMessage());

        RuleParseException e2 = assertThrows(RuleParseException.class,
                () -> ExpressionCompiler.compile("($amount > 1"));
        assertTrue(e2.getMessage().contains("右括号"), e2.getMessage());

        assertThrows(RuleParseException.class, () -> ExpressionCompiler.compile("$ > 1"));
        assertThrows(RuleParseException.class, () -> ExpressionCompiler.compile("$amount > 1 1"));
    }

    @Test
    void 运行时类型不匹配抛求值异常() {
        // 已被编译期类型检查拦截的场景，这里直接构造非法 AST 求值验证运行时防护
        Expr bad = new Expr.Binary(">",
                new Expr.Literal("abc"), new Expr.Literal(5.0));
        assertThrows(EvalException.class,
                () -> bad.eval(new Expr.EvalContext(event, Map.of())));
    }

    @Test
    void 引用未知字段与未注入窗口_求值报错() {
        Expr unknownField = ExpressionCompiler.compile("$notExist == 'x'");
        // 编译期类型检查能发现未知字段；直接绕开编译走 AST 求值
        assertThrows(EvalException.class,
                () -> unknownField.eval(new Expr.EvalContext(event, Map.of())));

        Expr fn = ExpressionCompiler.compile("count(w9) >= 1");
        assertThrows(EvalException.class,
                () -> fn.eval(new Expr.EvalContext(event, Map.of())));
    }

    @Test
    void 小数字面量() {
        assertEquals(0.5, ((Number) ExpressionCompiler.compile("0.5")
                .eval(new Expr.EvalContext(event, Map.of()))).doubleValue(), 1e-9);
    }
}
