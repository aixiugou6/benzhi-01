package com.risk.rule;

import com.risk.rule.expr.RuleParseException;
import com.risk.rule.model.Rule;
import com.risk.rule.model.RuleSet;
import com.risk.rule.model.WindowDef;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 模块一：规则模型与编译器。
 * 覆盖 JSON 解析、窗口/规则校验、AST 静态类型检查、优先级排序、坏文件整版拒绝。
 */
class RuleCompilerTest {

    private final RuleCompiler compiler = new RuleCompiler();

    private Rule rule(String id, int priority, String code, String condition) {
        Rule r = new Rule();
        r.setId(id);
        r.setPriority(priority);
        r.setReasonCode(code);
        r.setCondition(condition);
        return r;
    }

    @Test
    void 合法规则集编译成功并按优先级排序() {
        RuleSet rs = new RuleSet();
        rs.setVersion("v1");
        rs.setWindows(List.of(new WindowDef("w1", "cardNo", 60_000L)));
        rs.setRules(List.of(
                rule("low_prio", 20, "R_HIGH_FREQUENCY", "count(w1) >= 5"),
                rule("high_prio", 5, "R_AMOUNT_EXCEEDED", "$amount > 1000")));

        var compiled = compiler.compile(rs);
        assertEquals("v1", compiled.version());
        assertEquals(List.of("high_prio", "low_prio"),
                compiled.rules().stream().map(r -> r.id()).toList());
        assertTrue(compiled.referencedAliases().contains("w1"));
    }

    @Test
    void 同优先级按id字典序_保证求值顺序确定() {
        RuleSet rs = new RuleSet();
        rs.setVersion("v1");
        rs.setWindows(List.of(new WindowDef("w1", "cardNo", 60_000L)));
        rs.setRules(List.of(
                rule("zeta", 10, "R_HIGH_FREQUENCY", "count(w1) >= 9"),
                rule("alpha", 10, "R_HIGH_FREQUENCY", "count(w1) >= 5")));

        var compiled = compiler.compile(rs);
        assertEquals(List.of("alpha", "zeta"),
                compiled.rules().stream().map(r -> r.id()).toList());
    }

    @Test
    void 未引用的窗口不进入运行时记账集合() {
        RuleSet rs = new RuleSet();
        rs.setVersion("v1");
        rs.setWindows(List.of(
                new WindowDef("w1", "cardNo", 60_000L),
                new WindowDef("w2", "merchantId", 60_000L)));
        rs.setRules(List.of(rule("only_w1", 10, "X", "count(w1) >= 5")));

        var compiled = compiler.compile(rs);
        assertEquals(Set.of("w1"), compiled.referencedAliases());
    }

    @Test
    void 表达式引用未声明窗口_编译失败() {
        RuleSet rs = new RuleSet();
        rs.setVersion("v1");
        rs.setWindows(List.of());
        rs.setRules(List.of(rule("bad", 10, "X", "count(w9) >= 5")));
        RuleParseException ex = assertThrows(RuleParseException.class, () -> compiler.compile(rs));
        assertTrue(ex.getMessage().contains("未声明的窗口别名"), ex.getMessage());
    }

    @Test
    void 类型不匹配_数值与字符串比较_编译失败() {
        RuleSet rs = new RuleSet();
        rs.setVersion("v1");
        rs.setWindows(List.of());
        rs.setRules(List.of(rule("bad", 10, "X", "$amount > 'abc'")));
        RuleParseException ex = assertThrows(RuleParseException.class, () -> compiler.compile(rs));
        assertTrue(ex.getMessage().contains("类型不一致"), ex.getMessage());
    }

    @Test
    void 逻辑运算符两侧必须是布尔_编译失败() {
        RuleSet rs = new RuleSet();
        rs.setVersion("v1");
        rs.setWindows(List.of(new WindowDef("w1", "cardNo", 60_000L)));
        rs.setRules(List.of(rule("bad", 10, "X", "count(w1) AND $amount > 5")));
        assertThrows(RuleParseException.class, () -> compiler.compile(rs));
    }

    @Test
    void 重复规则id与非法分组维度_编译失败() {
        RuleSet rs = new RuleSet();
        rs.setVersion("v1");
        rs.setWindows(List.of(
                new WindowDef("w1", "notAField", 60_000L)));
        rs.setRules(List.of(
                rule("dup", 10, "X", "$amount > 1"),
                rule("dup", 20, "Y", "$amount > 2")));
        RuleParseException ex = assertThrows(RuleParseException.class, () -> compiler.compile(rs));
        assertTrue(ex.getMessage().contains("规则 id 重复"), ex.getMessage());
        assertTrue(ex.getMessage().contains("keyBy 不合法"), ex.getMessage());
    }

    @Test
    void json语法错误_编译失败() {
        RuleParseException ex = assertThrows(RuleParseException.class,
                () -> compiler.compileJson("{ not-json", "test"));
        assertTrue(ex.getMessage().contains("JSON 解析失败"), ex.getMessage());
    }

    @Test
    void 窗口长度非正与缺少version_编译失败() {
        RuleSet rs = new RuleSet();
        rs.setVersion(" ");
        rs.setWindows(List.of(new WindowDef("w1", "cardNo", 0)));
        rs.setRules(List.of());
        RuleParseException ex = assertThrows(RuleParseException.class, () -> compiler.compile(rs));
        assertTrue(ex.getMessage().contains("version 必填"));
        assertTrue(ex.getMessage().contains("windowMs 必须为正数"));
    }
}
