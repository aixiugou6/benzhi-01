package com.risk.rule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.risk.rule.expr.EvalException;
import com.risk.rule.expr.Expr;
import com.risk.rule.expr.ExpressionCompiler;
import com.risk.rule.expr.RuleParseException;
import com.risk.rule.model.CompiledRule;
import com.risk.rule.model.CompiledRuleSet;
import com.risk.rule.model.Rule;
import com.risk.rule.model.RuleSet;
import com.risk.rule.model.WindowDef;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 规则编译器：规则文件 JSON → 不可变 {@link CompiledRuleSet}。
 *
 * <p>编译期完成全部可静态发现的错误检查，热更新遇到坏文件时整版拒绝、线上继续跑旧版：
 * <ol>
 *   <li>JSON 结构与必填字段；</li>
 *   <li>窗口别名唯一、分组维度合法、窗口长度为正；</li>
 *   <li>规则 id/原因码/条件齐全，id 唯一；</li>
 *   <li>条件表达式词法/语法解析为 AST；</li>
 *   <li>AST 静态类型检查：AND/OR/NOT 两侧为布尔、比较两侧同为数值或同为字符串、
 *       窗口函数参数必须是已声明的窗口别名。</li>
 * </ol>
 */
public class RuleCompiler {

    private final ObjectMapper mapper;

    public RuleCompiler() {
        this(new ObjectMapper());
    }

    public RuleCompiler(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** 从文件编译（热更新监听的就是这个文件）。 */
    public CompiledRuleSet compileFile(Path path) throws IOException {
        return compileJson(Files.readString(path, StandardCharsets.UTF_8), path.toString());
    }

    /** 从 classpath 资源编译（打包内置默认规则）。 */
    public CompiledRuleSet compileResource(String resource) throws IOException {
        try (InputStream in = RuleCompiler.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("classpath 资源不存在: " + resource);
            }
            return compileJson(new String(in.readAllBytes(), StandardCharsets.UTF_8), "classpath:" + resource);
        }
    }

    /** 从 JSON 字符串编译；{@code source} 仅用于错误信息定位。 */
    public CompiledRuleSet compileJson(String json, String source) throws IOException {
        RuleSet raw;
        try {
            raw = mapper.readValue(json, RuleSet.class);
        } catch (IOException e) {
            throw new RuleParseException("规则文件 JSON 解析失败（" + source + "）: " + e.getMessage(), e);
        }
        return compile(raw, source);
    }

    /** 从内存模型编译（测试与程序化装配用）。 */
    public CompiledRuleSet compile(RuleSet raw) {
        return compile(raw, "<RuleSet>");
    }

    private CompiledRuleSet compile(RuleSet raw, String source) {
        List<String> errors = new ArrayList<>();

        if (raw.getVersion() == null || raw.getVersion().isBlank()) {
            errors.add("version 必填");
        }

        // ---- 窗口表 ----
        Map<String, WindowDef> windows = new HashMap<>();
        for (WindowDef w : raw.getWindows()) {
            String where = source + " / window[" + w.alias() + "]";
            if (w.alias() == null || w.alias().isBlank()) {
                errors.add(where + ": alias 必填");
                continue;
            }
            if (windows.containsKey(w.alias())) {
                errors.add(where + ": 窗口别名重复");
            }
            if (!WindowDef.isAllowedKeyBy(w.keyBy())) {
                errors.add(where + ": keyBy 不合法 '" + w.keyBy()
                        + "'，仅支持 cardNo/merchantId/txnType/currency");
            }
            if (w.windowMs() <= 0) {
                errors.add(where + ": windowMs 必须为正数");
            }
            windows.put(w.alias(), w);
        }

        // ---- 规则：解析 + 静态类型检查 + 排序 ----
        Set<String> ruleIds = new HashSet<>();
        List<CompiledRule> compiled = new ArrayList<>();
        for (Rule r : raw.getRules()) {
            String where = source + " / rule[" + r.getId() + "]";
            if (r.getId() == null || r.getId().isBlank()) {
                errors.add(where + ": id 必填");
                continue;
            }
            if (!ruleIds.add(r.getId())) {
                errors.add(where + ": 规则 id 重复");
            }
            if (!r.isEnabled()) {
                continue;
            }
            if (r.getReasonCode() == null || r.getReasonCode().isBlank()) {
                errors.add(where + ": reasonCode 必填");
            }
            if (r.getCondition() == null || r.getCondition().isBlank()) {
                errors.add(where + ": condition 必填");
                continue;
            }
            Expr ast;
            try {
                ast = ExpressionCompiler.compile(r.getCondition());
            } catch (RuleParseException e) {
                errors.add(where + ": 条件编译失败 — " + e.getMessage());
                continue;
            }
            try {
                TypeTag tag = typeCheck(ast, windows.keySet());
                if (tag != TypeTag.BOOL) {
                    errors.add(where + ": 条件表达式必须为布尔判断，实际为 " + tag.cn());
                }
            } catch (RuleParseException e) {
                errors.add(where + ": 类型检查失败 — " + e.getMessage());
                continue;
            }
            compiled.add(CompiledRule.of(r, ast));
        }

        if (!errors.isEmpty()) {
            throw new RuleParseException("规则文件存在 " + errors.size() + " 处错误：\n  - "
                    + String.join("\n  - ", errors));
        }

        compiled.sort(Comparator
                .comparingInt(CompiledRule::priority)
                .thenComparing(CompiledRule::id));

        // 仅被规则引用的窗口才需要在运行时记账（避免无关/可空分组维度误伤交易）
        Set<String> referenced = new HashSet<>();
        for (CompiledRule cr : compiled) {
            referenced.addAll(WindowReferences.collect(cr.condition()));
        }
        if (!windows.keySet().containsAll(referenced)) {
            // 理论上类型检查已拦截，防御性兜底
            referenced.removeIf(a -> !windows.containsKey(a));
        }

        return new CompiledRuleSet(raw.getVersion(), Map.copyOf(windows),
                List.copyOf(compiled), Set.copyOf(referenced));
    }

    // ------------------------------------------------------------------
    // 静态类型检查：推断 AST 的类型标签
    // ------------------------------------------------------------------

    private enum TypeTag {
        NUM("数值"), STRING("字符串"), BOOL("布尔");

        private final String cn;

        TypeTag(String cn) {
            this.cn = cn;
        }

        String cn() {
            return cn;
        }
    }

    private TypeTag typeCheck(Expr e, Set<String> aliases) {
        if (e instanceof Expr.Literal l) {
            return l.value() instanceof Number ? TypeTag.NUM : TypeTag.STRING;
        }
        if (e instanceof Expr.Field f) {
            return switch (f.name()) {
                case "amount" -> TypeTag.NUM;
                case "cardNo", "currency", "merchantId", "txnType", "eventId" -> TypeTag.STRING;
                default -> throw new RuleParseException("未知字段 $" + f.name());
            };
        }
        if (e instanceof Expr.Fn fn) {
            if (!fn.name().equals("count") && !fn.name().equals("sum")) {
                throw new RuleParseException("未知函数 " + fn.name() + "（仅支持 count/sum）");
            }
            if (fn.args().size() != 1) {
                throw new RuleParseException(fn.name() + " 只接受 1 个窗口别名参数");
            }
            Expr arg = fn.args().get(0);
            if (!(arg instanceof Expr.Literal l) || !(l.value() instanceof String alias)) {
                throw new RuleParseException(fn.name() + " 的参数必须是窗口别名，如 " + fn.name() + "(w1)");
            }
            if (!aliases.contains(alias)) {
                throw new RuleParseException(fn.name() + " 引用了未声明的窗口别名: " + alias);
            }
            return TypeTag.NUM;
        }
        if (e instanceof Expr.Not n) {
            TypeTag t = typeCheck(n.inner(), aliases);
            if (t != TypeTag.BOOL) {
                throw new RuleParseException("NOT 的操作数必须是布尔条件，实际为 " + t.cn());
            }
            return TypeTag.BOOL;
        }
        if (e instanceof Expr.Binary b) {
            TypeTag lt = typeCheck(b.left(), aliases);
            TypeTag rt = typeCheck(b.right(), aliases);
            if (b.op().equals("AND") || b.op().equals("OR")) {
                if (lt != TypeTag.BOOL || rt != TypeTag.BOOL) {
                    throw new RuleParseException(b.op() + " 两侧必须是布尔条件（" + lt.cn()
                            + " / " + rt.cn() + "）");
                }
                return TypeTag.BOOL;
            }
            // 比较运算：两侧必须同为数值或同为字符串
            if (lt != rt) {
                throw new RuleParseException("比较两侧类型不一致：" + lt.cn() + " 与 " + rt.cn());
            }
            if (lt != TypeTag.NUM && lt != TypeTag.STRING) {
                throw new RuleParseException("只能对数值或字符串做比较，实际为 " + lt.cn());
            }
            return TypeTag.BOOL;
        }
        throw new EvalException("编译器不支持的 AST 节点: " + e.getClass());
    }
}
