package com.risk.io;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risk.model.Decision;
import com.risk.model.TransactionEvent;

/**
 * 全引擎共用的 Jackson 映射：事件/决策均通过 Redis Stream 的 {@code json} 字段传输。
 */
public final class JsonCodecs {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonCodecs() {
    }

    /** 交易事件 → JSON（写入交易流）。 */
    public static String toJson(TransactionEvent e) {
        try {
            return MAPPER.writeValueAsString(e);
        } catch (Exception ex) {
            throw new IllegalStateException("事件序列化失败: " + e, ex);
        }
    }

    /** 交易流 JSON → 事件对象。 */
    public static TransactionEvent eventFromJson(String json) {
        try {
            return MAPPER.readValue(json, TransactionEvent.class);
        } catch (Exception ex) {
            throw new IllegalArgumentException("交易事件 JSON 解析失败: " + ex.getMessage(), ex);
        }
    }

    /** 决策 → JSON（写入决策流）。 */
    public static String toJson(Decision d) {
        try {
            return MAPPER.writeValueAsString(d);
        } catch (Exception ex) {
            throw new IllegalStateException("决策序列化失败", ex);
        }
    }
}
