package com.risk.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * 规则热更新 Source：周期性轮询规则文件，内容变化时把<b>文件原文</b>下发给广播流。
 *
 * <p>编译发生在下游每个算子上（各自 {@link com.risk.rule.RuleCompiler}），
 * 这样广播状态里只有字符串，规避 AST 在 Flink 状态序列化上的兼容风险。
 *
 * <p>变更检测依据“最后修改时间 + 文件大小”；轮询（而非 inotify）对容器挂载目录更稳妥。
 * 读到的文件原文不做校验——校验在编译侧统一进行，坏规则在那里被拒绝并保留旧版。
 */
public class RuleReloadSource extends RichSourceFunction<String> {

    private static final Logger log = LoggerFactory.getLogger(RuleReloadSource.class);

    private final String rulesFilePath;
    private final long pollIntervalMs;

    private transient volatile boolean running = true;
    private transient long lastModified = -1L;
    private transient long lastSize = -1L;

    public RuleReloadSource(String rulesFilePath, long pollIntervalMs) {
        this.rulesFilePath = rulesFilePath;
        this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public void open(Configuration parameters) {
        // 无外部资源
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 该函数会被 Flink 序列化分发，transient 字段反序列化后为 false，入口显式置位。
        running = true;
        Path path = Path.of(rulesFilePath);

        // 首次加载：文件尚不存在时等待（服务保持常驻，不退出）
        while (running && !Files.exists(path)) {
            log.warn("规则文件 {} 尚不存在，{}s 后重试", rulesFilePath, pollIntervalMs / 1000);
            sleepQuietly(pollIntervalMs);
        }

        loadAndEmit(ctx, path, true);

        while (running) {
            sleepQuietly(pollIntervalMs);
            if (!Files.exists(path)) {
                log.warn("规则文件被移除，下游继续保留当前版本");
                lastModified = -1L;
                lastSize = -1L;
                continue;
            }
            BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
            if (attr.lastModifiedTime().toMillis() != lastModified || attr.size() != lastSize) {
                loadAndEmit(ctx, path, false);
            }
        }
    }

    private void loadAndEmit(SourceContext<String> ctx, Path path, boolean initial)
            throws Exception {
        BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
        String content = Files.readString(path, StandardCharsets.UTF_8);
        synchronized (ctx.getCheckpointLock()) {
            ctx.collect(content);
            lastModified = attr.lastModifiedTime().toMillis();
            lastSize = attr.size();
        }
        log.info("{}规则文件已下发广播（{} 字节）", initial ? "初始" : "热更新", content.length());
    }

    private void sleepQuietly(long ms) throws InterruptedException {
        long deadline = System.currentTimeMillis() + ms;
        while (running && System.currentTimeMillis() < deadline) {
            Thread.sleep(Math.min(500, deadline - System.currentTimeMillis()));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
