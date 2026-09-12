package com.risk.rule.expr;

import com.risk.model.TransactionEvent;

import java.util.List;
import java.util.Map;

/**
 * 规则条件表达式的 AST 与求值逻辑。
 *
 * <p>表达式被 {@link com.risk.rule.RuleCompiler} 从字符串编译为 AST（sealed 类型层次），
 * 运行时在 {@link EvalContext} 上求值，不再做任何解析。
 *
 * <p>支持：
 * <ul>
 *   <li>字面量：数字 {@code 5}、{@code 1000.00}，字符串 {@code 'WITHDRAW'}；</li>
 *   <li>事件字段：{@code $amount}、{@code $txnType}、{@code $currency}、{@code $merchantId}、{@code $cardNo}；</li>
 *   <li>窗口函数：{@code count(w1)}（窗口内次数，含本次）、{@code sum(w1)}（窗口内金额合计，含本次）；</li>
 *   <li>比较：{@code > >= < <= == !=}（数值与数值比、字符串与字符串比）；</li>
 *   <li>布尔：{@code AND OR NOT} 与括号。</li>
 * </ul>
 * 例：{@code count(w1) >= 5 AND $amount > 10000}
 */
public sealed interface Expr permits Expr.Literal, Expr.Field, Expr.Fn, Expr.Binary, Expr.Not {

    /** 通用求值：返回 Number / String / Boolean。 */
    Object eval(EvalContext ctx);

    /** 求值为数值，类型不符抛 {@link EvalException}。 */
    default double evalNumber(EvalContext ctx) {
        Object v = eval(ctx);
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        throw new EvalException("期望数值类型，实际为 " + TypeNames.of(v));
    }

    /** 求值为布尔，类型不符抛 {@link EvalException}。 */
    default boolean evalBoolean(EvalContext ctx) {
        Object v = eval(ctx);
        if (v instanceof Boolean b) {
            return b;
        }
        throw new EvalException("期望布尔类型，实际为 " + TypeNames.of(v));
    }

    /** 常量字面量。 */
    record Literal(Object value) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            return value;
        }
    }

    /** 事件字段引用，如 {@code $amount}。 */
    record Field(String name) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            return ctx.field(name);
        }
    }

    /** 窗口统计函数，如 {@code count(w1)}。 */
    record Fn(String name, List<Expr> args) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            return ctx.callFunction(name, args);
        }
    }

    /** 二元运算：比较或 AND/OR。 */
    record Binary(String op, Expr left, Expr right) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            return switch (op) {
                case "AND" -> left.evalBoolean(ctx) && right.evalBoolean(ctx);
                case "OR" -> left.evalBoolean(ctx) || right.evalBoolean(ctx);
                default -> compare(ctx);
            };
        }

        private boolean compare(EvalContext ctx) {
            Object a = left.eval(ctx);
            Object b = right.eval(ctx);
            if (a instanceof Number na && b instanceof Number nb) {
                double x = na.doubleValue();
                double y = nb.doubleValue();
                return switch (op) {
                    case ">" -> x > y;
                    case ">=" -> x >= y;
                    case "<" -> x < y;
                    case "<=" -> x <= y;
                    case "==", "=" -> x == y;
                    case "!=" -> x != y;
                    default -> throw new EvalException("不支持的运算符: " + op);
                };
            }
            if (a instanceof String sa && b instanceof String sb) {
                int c = sa.compareTo(sb);
                return switch (op) {
                    case "==", "=" -> sa.equals(sb);
                    case "!=" -> !sa.equals(sb);
                    case ">" -> c > 0;
                    case ">=" -> c >= 0;
                    case "<" -> c < 0;
                    case "<=" -> c <= 0;
                    default -> throw new EvalException("字符串不支持的运算符: " + op);
                };
            }
            throw new EvalException("比较两侧类型不匹配: " + TypeNames.of(a) + " 与 " + TypeNames.of(b));
        }
    }

    /** 逻辑非。 */
    record Not(Expr inner) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            return !inner.evalBoolean(ctx);
        }
    }

    /** 类型名辅助（异常信息用）。 */
    final class TypeNames {
        static String of(Object v) {
            if (v == null) return "null";
            if (v instanceof Number) return "数值";
            if (v instanceof Boolean) return "布尔";
            if (v instanceof String) return "字符串";
            return v.getClass().getSimpleName();
        }

        private TypeNames() {
        }
    }

    /**
     * 求值上下文：绑定当前交易事件与各窗口的统计结果。
     *
     * <p>窗口统计由引擎在求值前通过 Redis Lua 原子计算并注入，{@code count/sum} 直接读取，
     * 函数内部不做 IO，保证求值是纯函数、可单测。
     */
    final class EvalContext {
        private final TransactionEvent event;
        private final Map<String, WindowStats> statsByAlias;

        public EvalContext(TransactionEvent event, Map<String, WindowStats> statsByAlias) {
            this.event = event;
            this.statsByAlias = statsByAlias;
        }

        /** 读取事件字段。 */
        Object field(String name) {
            return switch (name) {
                case "amount" -> event.getAmount();
                case "cardNo" -> event.getCardNo();
                case "currency" -> event.getCurrency();
                case "merchantId" -> event.getMerchantId();
                case "txnType" -> event.getTxnType();
                case "eventId" -> event.getEventId();
                default -> throw new EvalException("未知字段: $" + name
                        + "（支持 amount/cardNo/currency/merchantId/txnType/eventId）");
            };
        }

        /** 窗口函数调用。 */
        Object callFunction(String name, List<Expr> args) {
            if (args.size() != 1) {
                throw new EvalException("函数 " + name + " 只接受 1 个窗口别名参数");
            }
            Object arg = args.get(0).eval(this);
            if (!(arg instanceof String alias)) {
                throw new EvalException("函数 " + name + " 的参数必须是窗口别名");
            }
            WindowStats stats = statsByAlias.get(alias);
            if (stats == null) {
                throw new EvalException("表达式引用了未声明的窗口别名: " + alias);
            }
            return switch (name) {
                case "count" -> stats.count();
                case "sum" -> stats.sumAmount();
                default -> throw new EvalException("未知函数: " + name + "（支持 count/sum）");
            };
        }
    }
}
