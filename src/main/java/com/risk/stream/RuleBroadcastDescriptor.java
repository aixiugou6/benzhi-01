package com.risk.stream;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * 广播状态描述符：只保留一个 key {@link #RULESET_KEY}，值为规则文件原文（JSON）。
 */
public final class RuleBroadcastDescriptor {

    public static final String RULESET_KEY = "rules.json";

    public static final MapStateDescriptor<String, String> RULESET_STATE =
            new MapStateDescriptor<>(
                    "ruleset-broadcast",
                    TypeInformation.of(String.class),
                    TypeInformation.of(String.class));

    private RuleBroadcastDescriptor() {
    }
}
