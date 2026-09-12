package com.risk.window;

import com.risk.model.TransactionEvent;
import com.risk.rule.model.WindowDef;

/**
 * 按 {@link WindowDef#keyBy()} 从事件中取分组键值。
 * 分组字段为空时返回 null（由上层决定是否跳过该窗口的统计与求值）。
 */
public final class GroupKeyResolver {

    private GroupKeyResolver() {
    }

    public static String resolve(WindowDef window, TransactionEvent event) {
        String raw = switch (window.keyBy()) {
            case "cardNo" -> event.getCardNo();
            case "merchantId" -> event.getMerchantId();
            case "txnType" -> event.getTxnType();
            case "currency" -> event.getCurrency();
            default -> throw new IllegalArgumentException("不支持的分组维度: " + window.keyBy());
        };
        return (raw == null || raw.isBlank()) ? null : raw;
    }
}
