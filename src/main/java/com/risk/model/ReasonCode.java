package com.risk.model;

/**
 * 原因码：决策结论的机器可读解释。
 *
 * <p>命名约定：{@code R_*} 为规则拦截码，由规则文件中的 {@code reasonCode} 引用；
 * {@code SYS_*} 为引擎自身产生的码。交易链路可按码做差异化处理（告警、转人工等）。
 */
public enum ReasonCode {

    /** 无任何规则命中，放行。 */
    NONE("无风险，放行"),

    /** 短时高频：同一维度在滑动窗口内次数达到阈值。 */
    R_HIGH_FREQUENCY("短时高频交易"),

    /** 金额异常：单笔金额达到阈值。 */
    R_AMOUNT_EXCEEDED("单笔金额异常"),

    /** 窗口内累计金额异常。 */
    R_WINDOW_AMOUNT_EXCEEDED("窗口期累计金额异常"),

    /** 规则文件中自定义码的兜底承载（规则可声明系统未枚举的码字符串）。 */
    R_CUSTOM("自定义规则拦截"),

    /** 引擎级：输入事件不合法。 */
    SYS_INVALID_EVENT("事件不合法"),

    /** 引擎级：规则求值过程中发生内部错误。fail-closed，默认拦截。 */
    SYS_ENGINE_ERROR("引擎内部错误");

    private final String description;

    ReasonCode(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    /**
     * 把规则文件中的码字符串解析为枚举；未枚举的码归一到 {@link #R_CUSTOM}。
     *
     * @param code 规则文件声明的原因码（可为 null）
     */
    public static ReasonCode fromCode(String code) {
        if (code == null || code.isBlank()) {
            return R_CUSTOM;
        }
        try {
            return ReasonCode.valueOf(code);
        } catch (IllegalArgumentException e) {
            return R_CUSTOM;
        }
    }
}
