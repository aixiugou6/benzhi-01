package com.risk.rule.expr;

import com.risk.rule.expr.Lexer.Kind;
import com.risk.rule.expr.Lexer.Token;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 递归下降语法分析器：把 token 流解析为 {@link Expr} AST。
 *
 * <p>文法（优先级从低到高）：
 * <pre>
 *   expr       := orExpr
 *   orExpr     := andExpr ( OR andExpr )*
 *   andExpr    := notExpr ( AND notExpr )*
 *   notExpr    := NOT notExpr | comparison
 *   comparison := primary ( OP primary )?        OP: &gt; &gt;= &lt; &lt;= == = !=
 *   primary    := NUMBER | STRING | FIELD
 *               | IDENT '(' expr ')'             窗口函数调用
 *               | IDENT                          窗口别名（作为函数实参）
 *               | '(' expr ')'
 * </pre>
 * 关键字 AND/OR/NOT 大小写不敏感。
 */
final class Parser {

    private static final Set<String> COMPARE_OPS = Set.of(">", ">=", "<", "<=", "==", "=", "!=");

    private final List<Token> tokens;
    private int idx;

    private Parser(List<Token> tokens) {
        this.tokens = tokens;
    }

    /** 解析入口。 */
    static Expr parse(String text) {
        if (text == null || text.isBlank()) {
            throw new RuleParseException("条件表达式为空");
        }
        Parser p = new Parser(new Lexer(text).tokenize());
        Expr e = p.parseOr();
        p.expectEnd();
        return e;
    }

    private Expr parseOr() {
        Expr left = parseAnd();
        while (isKeyword("OR")) {
            idx++;
            Expr right = parseAnd();
            left = new Expr.Binary("OR", left, right);
        }
        return left;
    }

    private Expr parseAnd() {
        Expr left = parseNot();
        while (isKeyword("AND")) {
            idx++;
            Expr right = parseNot();
            left = new Expr.Binary("AND", left, right);
        }
        return left;
    }

    private Expr parseNot() {
        if (isKeyword("NOT")) {
            idx++;
            return new Expr.Not(parseNot());
        }
        return parseComparison();
    }

    private Expr parseComparison() {
        Expr left = parsePrimary();
        Token t = peek();
        if (t.kind() == Kind.OP && COMPARE_OPS.contains(t.text())) {
            idx++;
            Expr right = parsePrimary();
            String op = "=".equals(t.text()) ? "==" : t.text();
            return new Expr.Binary(op, left, right);
        }
        return left;
    }

    private Expr parsePrimary() {
        Token t = peek();
        switch (t.kind()) {
            case NUMBER -> {
                idx++;
                double d;
                try {
                    d = Double.parseDouble(t.text());
                } catch (NumberFormatException e) {
                    throw new RuleParseException("非法数字 '" + t.text() + "'（位置 " + t.pos() + "）");
                }
                return new Expr.Literal(d);
            }
            case STRING -> {
                idx++;
                return new Expr.Literal(t.text());
            }
            case FIELD -> {
                idx++;
                return new Expr.Field(t.text());
            }
            case LPAREN -> {
                idx++;
                Expr e = parseOr();
                expect(Kind.RPAREN, "缺少右括号");
                idx++;
                return e;
            }
            case IDENT -> {
                // 函数调用 或 窗口别名
                if (tokens.get(idx + 1).kind() == Kind.LPAREN) {
                    idx += 2;
                    List<Expr> args = new ArrayList<>();
                    if (peek().kind() != Kind.RPAREN) {
                        args.add(parseOr());
                        while (peek().kind() == Kind.COMMA) {
                            idx++;
                            args.add(parseOr());
                        }
                    }
                    expect(Kind.RPAREN, "函数调用缺少右括号");
                    idx++;
                    return new Expr.Fn(t.text(), args);
                }
                idx++;
                // 裸标识符仅允许作为窗口别名：以字符串字面量形式进入 AST
                return new Expr.Literal(t.text());
            }
            default -> throw new RuleParseException("未预期的 token '" + t.text()
                    + "'（位置 " + t.pos() + "）");
        }
    }

    private boolean isKeyword(String kw) {
        Token t = peek();
        return t.kind() == Kind.IDENT && t.text().equalsIgnoreCase(kw);
    }

    private Token peek() {
        return tokens.get(idx);
    }

    private void expect(Kind kind, String msg) {
        if (peek().kind() != kind) {
            Token t = peek();
            throw new RuleParseException(msg + "，实际为 '" + t.text() + "'（位置 " + t.pos() + "）");
        }
    }

    private void expectEnd() {
        Token t = peek();
        if (t.kind() != Kind.EOF) {
            throw new RuleParseException("表达式尾部存在多余内容 '" + t.text()
                    + "'（位置 " + t.pos() + "）");
        }
    }
}
