package com.risk.rule;

import com.risk.rule.expr.Expr;

import java.util.ArrayList;
import java.util.List;

/**
 * 收集条件 AST 中引用的窗口别名（按首次出现顺序、去重）。
 * 用于：引擎预取所需窗口统计、命中后在决策中回填 windowCount。
 */
public final class WindowReferences {

    private WindowReferences() {
    }

    public static List<String> collect(Expr e) {
        List<String> out = new ArrayList<>();
        walk(e, out);
        return out;
    }

    private static void walk(Expr e, List<String> out) {
        if (e instanceof Expr.Fn fn) {
            for (Expr a : fn.args()) {
                if (a instanceof Expr.Literal l && l.value() instanceof String alias) {
                    if (!out.contains(alias)) {
                        out.add(alias);
                    }
                }
            }
            fn.args().forEach(a -> walk(a, out));
        } else if (e instanceof Expr.Binary b) {
            walk(b.left(), out);
            walk(b.right(), out);
        } else if (e instanceof Expr.Not n) {
            walk(n.inner(), out);
        }
    }
}
