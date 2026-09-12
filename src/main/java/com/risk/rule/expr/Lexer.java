package com.risk.rule.expr;

import java.util.ArrayList;
import java.util.List;

/**
 * 表达式词法分析器。
 *
 * <p>Token 类别：关键字（AND/OR/NOT）、比较符、括号、逗号、数字字面量、
 * 单/双引号字符串字面量、{@code $} 字段引用、普通标识符（函数名/窗口别名）。
 * 词法错误直接抛 {@link RuleParseException} 并带位置。
 */
final class Lexer {

    enum Kind { NUMBER, STRING, FIELD, IDENT, OP, LPAREN, RPAREN, COMMA, EOF }

    record Token(Kind kind, String text, int pos) {
    }

    private final String src;
    private int pos;

    Lexer(String src) {
        this.src = src;
        this.pos = 0;
    }

    List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (Character.isWhitespace(c)) {
                pos++;
            } else if (c == '(') {
                tokens.add(new Token(Kind.LPAREN, "(", pos++));
            } else if (c == ')') {
                tokens.add(new Token(Kind.RPAREN, ")", pos++));
            } else if (c == ',') {
                tokens.add(new Token(Kind.COMMA, ",", pos++));
            } else if (c == '\'' || c == '"') {
                tokens.add(readString(c));
            } else if (c == '$') {
                tokens.add(readField());
            } else if (Character.isDigit(c) || (c == '.' && pos + 1 < src.length()
                    && Character.isDigit(src.charAt(pos + 1)))) {
                tokens.add(readNumber());
            } else if (isIdentStart(c)) {
                tokens.add(readIdent());
            } else if (c == '>' || c == '<' || c == '=' || c == '!') {
                tokens.add(readOperator());
            } else {
                throw new RuleParseException("无法识别的字符 '" + c + "'（位置 " + pos + "）");
            }
        }
        tokens.add(new Token(Kind.EOF, "", pos));
        return tokens;
    }

    private Token readString(char quote) {
        int start = pos;
        pos++; // 跳过引号
        StringBuilder sb = new StringBuilder();
        while (pos < src.length() && src.charAt(pos) != quote) {
            char c = src.charAt(pos);
            if (c == '\\' && pos + 1 < src.length()) {
                char n = src.charAt(pos + 1);
                sb.append(switch (n) {
                    case 'n' -> '\n';
                    case 't' -> '\t';
                    default -> n;
                });
                pos += 2;
            } else {
                sb.append(c);
                pos++;
            }
        }
        if (pos >= src.length()) {
            throw new RuleParseException("字符串未闭合（起始位置 " + start + "）");
        }
        pos++; // 跳过结束引号
        return new Token(Kind.STRING, sb.toString(), start);
    }

    private Token readField() {
        int start = pos;
        pos++; // 跳过 $
        int nameStart = pos;
        while (pos < src.length() && isIdentPart(src.charAt(pos))) {
            pos++;
        }
        if (nameStart == pos) {
            throw new RuleParseException("$ 后缺少字段名（位置 " + start + "）");
        }
        return new Token(Kind.FIELD, src.substring(nameStart, pos), start);
    }

    private Token readNumber() {
        int start = pos;
        int dots = 0;
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (Character.isDigit(c)) {
                pos++;
            } else if (c == '.') {
                if (++dots > 1) {
                    throw new RuleParseException("非法数字（多个小数点，位置 " + pos + "）");
                }
                pos++;
            } else {
                break;
            }
        }
        return new Token(Kind.NUMBER, src.substring(start, pos), start);
    }

    private Token readIdent() {
        int start = pos;
        while (pos < src.length() && isIdentPart(src.charAt(pos))) {
            pos++;
        }
        return new Token(Kind.IDENT, src.substring(start, pos), start);
    }

    private Token readOperator() {
        int start = pos;
        char c = src.charAt(pos);
        if ((c == '>' || c == '<' || c == '=' || c == '!')
                && pos + 1 < src.length() && src.charAt(pos + 1) == '=') {
            pos += 2;
            return new Token(Kind.OP, src.substring(start, pos), start);
        }
        if (c == '=') {
            pos++;
            return new Token(Kind.OP, "=", start);
        }
        if (c == '>' || c == '<') {
            pos++;
            return new Token(Kind.OP, String.valueOf(c), start);
        }
        throw new RuleParseException("未预期的字符 '" + c + "'（位置 " + pos + "）");
    }

    private static boolean isIdentStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}
