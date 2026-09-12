package com.risk.engine;

import java.util.Arrays;
import java.util.List;

/**
 * 可控时钟：按预置时间序列依次返回（模拟交易时间推进，精确验证窗口边界）。
 * 取完预置值后，固定返回最后一个值并按步长自动推进。
 */
public class ScriptedClock implements Clock {

    private final List<Long> script;
    private int idx;

    public ScriptedClock(long... times) {
        this.script = Arrays.stream(times).boxed().toList();
    }

    @Override
    public long nowMs() {
        long v = script.get(Math.min(idx, script.size() - 1));
        idx++;
        return v;
    }
}
