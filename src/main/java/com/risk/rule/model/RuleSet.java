package com.risk.rule.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * 规则集：规则文件（默认 {@code config/rules.json}）的顶层结构。
 *
 * <pre>
 * {
 *   "version": "rules-001",
 *   "windows": [ {"alias":"w1","keyBy":"cardNo","windowMs":60000} ],
 *   "rules":  [ { ... } ]
 * }
 * </pre>
 *
 * 整个文件被原子替换后热生效，处理中的事件仍用旧版本，保证一致性。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleSet {

    /** 规则集版本号，会原样写入每条决策，便于回溯“当时用了哪版规则”。 */
    private String version;

    /** 本规则集用到的全部滑动窗口。 */
    private List<WindowDef> windows = List.of();

    /** 规则列表，求值顺序由编译期按 priority 排序决定，与文件中的书写顺序无关。 */
    private List<Rule> rules = List.of();

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public List<WindowDef> getWindows() {
        return windows;
    }

    public void setWindows(List<WindowDef> windows) {
        this.windows = windows == null ? List.of() : windows;
    }

    public List<Rule> getRules() {
        return rules;
    }

    public void setRules(List<Rule> rules) {
        this.rules = rules == null ? List.of() : rules;
    }
}
