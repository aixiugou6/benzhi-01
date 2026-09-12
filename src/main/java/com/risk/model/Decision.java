package com.risk.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * 风控决策结果（引擎输出）。标准可变 POJO（含无参构造），以便 Flink PojoSerializer 在算子间传输、
 * Jackson 在决策流上序列化/反序列化。
 *
 * <p>以 JSON 写入决策 Redis Stream（默认 {@code risk.decisions}），交易链路据此放行或拒绝：
 * <pre>
 * {"eventId":"E1","cardNo":"CARD-001","action":"BLOCK",
 *  "reasonCode":"R_HIGH_FREQUENCY","reason":"短时高频交易",
 *  "matchedRuleId":"high_freq_1m_5","priority":10,"windowCount":5,
 *  "ruleSetVersion":"rules-001","decisionTimeMs":1726000000123}
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Decision {

    /** 动作 PASS/BLOCK/INVALID。 */
    private DecisionAction action;
    /** 原因码字符串（保留规则文件中的原始码，可能是枚举外的自定义码）。 */
    private String reasonCode;
    /** 人可读原因。 */
    private String reason;
    /** 命中的规则 ID；放行/非法时为 null。 */
    private String matchedRuleId;
    /** 命中规则的优先级。 */
    private Integer priority;
    /** 求值时命中规则的窗口计数（含本次），无窗口统计时为 null。 */
    private Long windowCount;
    /** 对应交易事件 ID。 */
    private String eventId;
    /** 对应卡号。 */
    private String cardNo;
    /** 做出该决策时使用的规则集版本。 */
    private String ruleSetVersion;
    /** 决策时间（epoch 毫秒）。 */
    private long decisionTimeMs;

    public Decision() {
    }

    public Decision(DecisionAction action, String reasonCode, String reason, String matchedRuleId,
                    Integer priority, Long windowCount, String eventId, String cardNo,
                    String ruleSetVersion, long decisionTimeMs) {
        this.action = action;
        this.reasonCode = reasonCode;
        this.reason = reason;
        this.matchedRuleId = matchedRuleId;
        this.priority = priority;
        this.windowCount = windowCount;
        this.eventId = eventId;
        this.cardNo = cardNo;
        this.ruleSetVersion = ruleSetVersion;
        this.decisionTimeMs = decisionTimeMs;
    }

    /** 快速构造放行决策。 */
    public static Decision pass(String eventId, String cardNo, String ruleSetVersion, long nowMs) {
        return new Decision(DecisionAction.PASS, ReasonCode.NONE.name(),
                ReasonCode.NONE.getDescription(), null, null, null,
                eventId, cardNo, ruleSetVersion, nowMs);
    }

    /** 快速构造拦截决策。 */
    public static Decision block(String eventId, String cardNo, String ruleSetVersion, long nowMs,
                                 String ruleId, int priority, String rawReasonCode,
                                 String reason, Long windowCount) {
        return new Decision(DecisionAction.BLOCK, rawReasonCode, reason,
                ruleId, priority, windowCount, eventId, cardNo, ruleSetVersion, nowMs);
    }

    /** 快速构造非法事件决策。 */
    public static Decision invalid(TransactionEvent event, String detail, long nowMs) {
        return new Decision(DecisionAction.INVALID, ReasonCode.SYS_INVALID_EVENT.name(),
                ReasonCode.SYS_INVALID_EVENT.getDescription() + ": " + detail,
                null, null, null,
                event == null ? null : event.getEventId(),
                event == null ? null : event.getCardNo(),
                null, nowMs);
    }

    /** 是否应拒绝交易（拦截拒绝；INVALID 由链路按校验失败处理）。 */
    public boolean rejected() {
        return action == DecisionAction.BLOCK;
    }

    public DecisionAction getAction() {
        return action;
    }

    public void setAction(DecisionAction action) {
        this.action = action;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public void setReasonCode(String reasonCode) {
        this.reasonCode = reasonCode;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getMatchedRuleId() {
        return matchedRuleId;
    }

    public void setMatchedRuleId(String matchedRuleId) {
        this.matchedRuleId = matchedRuleId;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public Long getWindowCount() {
        return windowCount;
    }

    public void setWindowCount(Long windowCount) {
        this.windowCount = windowCount;
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

    public String getRuleSetVersion() {
        return ruleSetVersion;
    }

    public void setRuleSetVersion(String ruleSetVersion) {
        this.ruleSetVersion = ruleSetVersion;
    }

    public long getDecisionTimeMs() {
        return decisionTimeMs;
    }

    public void setDecisionTimeMs(long decisionTimeMs) {
        this.decisionTimeMs = decisionTimeMs;
    }
}
