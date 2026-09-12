package com.risk.rule.expr;

/**
 * 一个滑动窗口在“包含当前交易”后的统计快照。
 *
 * @param count     窗口内交易笔数（含当前这笔）
 * @param sumAmount 窗口内交易金额合计（含当前这笔）
 */
public record WindowStats(long count, double sumAmount) {
}
