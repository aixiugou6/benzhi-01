package com.risk.rule.model;

/**
 * 滑动窗口定义。
 *
 * @param alias    窗口别名，在规则表达式中被 {@code count(w1)}/{@code sum(w1)} 引用，规则集内唯一
 * @param keyBy    分组维度（按事件字段名）：cardNo / merchantId / txnType / currency
 * @param windowMs 窗口长度（毫秒），必须 &gt; 0
 */
public record WindowDef(String alias, String keyBy, long windowMs) {

    /** 允许作为分组维度的事件字段。 */
    public static boolean isAllowedKeyBy(String keyBy) {
        return switch (keyBy) {
            case "cardNo", "merchantId", "txnType", "currency" -> true;
            default -> false;
        };
    }
}
