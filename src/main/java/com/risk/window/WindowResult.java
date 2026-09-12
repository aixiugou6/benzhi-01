package com.risk.window;

/**
 * 一次窗口记账的返回结果。
 *
 * @param count      窗口内笔数（含本次；重复 eventId 不重复计数）
 * @param sumAmount  窗口内金额合计（含本次）
 * @param newlyAdded 本次是否为新事件（false 表示重复投递被幂等去重）
 * @param usedNowMs  本次记账使用的时间戳（注入时间或 Redis 节点 {@code TIME}）
 */
public record WindowResult(long count, double sumAmount, boolean newlyAdded, long usedNowMs) {
}
