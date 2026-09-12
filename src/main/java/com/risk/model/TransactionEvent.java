package com.risk.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * 交易事件：风控引擎的输入。
 *
 * <p>由支付链路在用户点击支付后产生，以 JSON 形式写入 Redis Stream（字段 {@code json}），
 * 例如：
 * <pre>
 * XADD risk.transactions '*' json '{"eventId":"E1","cardNo":"CARD-001","amount":100.00,
 * "currency":"CNY","merchantId":"M-7","txnType":"PURCHASE","eventTimeMs":1726000000000}'
 * </pre>
 *
 * <p>必填字段：{@code eventId}（全局唯一，幂等键）、{@code cardNo}（默认分组维度）、
 * {@code amount}（金额，单位元）。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransactionEvent {

    /** 交易事件唯一 ID（支付链路生成），同时作为滑动窗口去重的幂等键。 */
    private String eventId;

    /** 卡号 / 支付令牌，短时高频等规则的默认分组维度。 */
    private String cardNo;

    /** 交易金额，单位元，必须为非负数。 */
    private double amount;

    /** 币种，如 CNY。 */
    private String currency;

    /** 商户 ID。 */
    private String merchantId;

    /** 交易类型，如 PURCHASE / WITHDRAW。 */
    private String txnType;

    /** 事件发生时间（epoch 毫秒）；为空时由引擎按接收时间补齐。 */
    private Long eventTimeMs;

    public TransactionEvent() {
    }

    public TransactionEvent(String eventId, String cardNo, double amount,
                            String currency, String merchantId, String txnType, Long eventTimeMs) {
        this.eventId = eventId;
        this.cardNo = cardNo;
        this.amount = amount;
        this.currency = currency;
        this.merchantId = merchantId;
        this.txnType = txnType;
        this.eventTimeMs = eventTimeMs;
    }

    /** 入口数据校验：返回错误描述，合法返回 null。 */
    public String validate() {
        if (eventId == null || eventId.isBlank()) {
            return "eventId is required";
        }
        if (cardNo == null || cardNo.isBlank()) {
            return "cardNo is required";
        }
        if (amount < 0 || Double.isNaN(amount) || Double.isInfinite(amount)) {
            return "amount must be a finite non-negative number";
        }
        return null;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCardNo() {
        return cardNo;
    }

    public void setCardNo(String cardNo) {
        this.cardNo = cardNo;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getTxnType() {
        return txnType;
    }

    public void setTxnType(String txnType) {
        this.txnType = txnType;
    }

    public Long getEventTimeMs() {
        return eventTimeMs;
    }

    public void setEventTimeMs(Long eventTimeMs) {
        this.eventTimeMs = eventTimeMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionEvent that)) return false;
        return Double.compare(that.amount, amount) == 0
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(cardNo, that.cardNo)
                && Objects.equals(currency, that.currency)
                && Objects.equals(merchantId, that.merchantId)
                && Objects.equals(txnType, that.txnType)
                && Objects.equals(eventTimeMs, that.eventTimeMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, cardNo, amount, currency, merchantId, txnType, eventTimeMs);
    }

    @Override
    public String toString() {
        return "TransactionEvent{eventId='" + eventId + "', cardNo='" + cardNo
                + "', amount=" + amount + ", merchantId='" + merchantId + "'}";
    }
}
