package com.risk.engine;

/**
 * 时钟抽象：引擎所有时间（窗口记账、决策时间戳）都经由此处获取，
 * 生产用系统时钟，测试可注入任意时间以验证窗口边界。
 */
@FunctionalInterface
public interface Clock {

    /** 当前时间（epoch 毫秒）。 */
    long nowMs();

    /** 系统 UTC 时钟。 */
    static Clock system() {
        return System::currentTimeMillis;
    }
}
