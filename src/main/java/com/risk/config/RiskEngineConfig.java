package com.risk.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 引擎运行配置（{@code config/application.json}）。
 *
 * <p>所有项都可用环境变量覆盖（容器部署常用），环境变量名见各字段注释。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RiskEngineConfig implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    /** Redis 主机；环境变量 {@code RISK_REDIS_HOST}。 */
    private String redisHost = "127.0.0.1";

    /** Redis 端口；环境变量 {@code RISK_REDIS_PORT}。 */
    private int redisPort = 6379;

    /** Redis 密码，没有留 null；环境变量 {@code RISK_REDIS_PASSWORD}。 */
    private String redisPassword = null;

    /** Redis 数据库序号；环境变量 {@code RISK_REDIS_DB}。 */
    private int redisDatabase = 0;

    /** 交易事件 Redis Stream 名；env {@code RISK_TXN_STREAM}。 */
    private String transactionStream = "risk.transactions";

    /** 决策结果 Redis Stream 名；env {@code RISK_DECISION_STREAM}。 */
    private String decisionStream = "risk.decisions";

    /** 消费组名；env {@code RISK_CONSUMER_GROUP}。 */
    private String consumerGroup = "risk-engine";

    /** 规则文件路径（文件系统）；env {@code RISK_RULES_FILE}。 */
    private String rulesFile = "config/rules.json";

    /** 规则文件轮询间隔（秒）；env {@code RISK_RULES_RELOAD_SECONDS}。 */
    private int rulesReloadSeconds = 5;

    /** Flink 并行度；env {@code RISK_PARALLELISM}。 */
    private int parallelism = 1;

    /** 决策流最大保留条数（XADD MAXLEN 近似裁剪，防止无界增长）。 */
    private long decisionStreamMaxLen = 1_000_000L;

    /** 从 classpath 读取配置文件（仓库内置默认配置）。 */
    public static RiskEngineConfig load(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        RiskEngineConfig cfg;
        Path p = Path.of(path);
        if (Files.exists(p)) {
            cfg = mapper.readValue(Files.readString(p, StandardCharsets.UTF_8), RiskEngineConfig.class);
        } else {
            try (InputStream in = RiskEngineConfig.class.getClassLoader().getResourceAsStream(path)) {
                if (in == null) {
                    throw new IOException("配置文件不存在: " + path);
                }
                cfg = mapper.readValue(in.readAllBytes(), RiskEngineConfig.class);
            }
        }
        cfg.applyEnvOverrides();
        return cfg;
    }

    /** 环境变量覆盖（存在且非空才覆盖）。 */
    private void applyEnvOverrides() {
        redisHost = override("RISK_REDIS_HOST", redisHost);
        redisPort = overrideInt("RISK_REDIS_PORT", redisPort);
        redisPassword = override("RISK_REDIS_PASSWORD", redisPassword);
        redisDatabase = overrideInt("RISK_REDIS_DB", redisDatabase);
        transactionStream = override("RISK_TXN_STREAM", transactionStream);
        decisionStream = override("RISK_DECISION_STREAM", decisionStream);
        consumerGroup = override("RISK_CONSUMER_GROUP", consumerGroup);
        rulesFile = override("RISK_RULES_FILE", rulesFile);
        rulesReloadSeconds = overrideInt("RISK_RULES_RELOAD_SECONDS", rulesReloadSeconds);
        parallelism = overrideInt("RISK_PARALLELISM", parallelism);
        decisionStreamMaxLen = overrideLong("RISK_DECISION_STREAM_MAXLEN", decisionStreamMaxLen);
    }

    private static String override(String env, String dflt) {
        String v = System.getenv(env);
        return (v == null || v.isBlank()) ? dflt : v;
    }

    private static int overrideInt(String env, int dflt) {
        String v = System.getenv(env);
        return (v == null || v.isBlank()) ? dflt : Integer.parseInt(v);
    }

    private static long overrideLong(String env, long dflt) {
        String v = System.getenv(env);
        return (v == null || v.isBlank()) ? dflt : Long.parseLong(v);
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public void setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    public int getRedisDatabase() {
        return redisDatabase;
    }

    public void setRedisDatabase(int redisDatabase) {
        this.redisDatabase = redisDatabase;
    }

    public String getTransactionStream() {
        return transactionStream;
    }

    public void setTransactionStream(String transactionStream) {
        this.transactionStream = transactionStream;
    }

    public String getDecisionStream() {
        return decisionStream;
    }

    public void setDecisionStream(String decisionStream) {
        this.decisionStream = decisionStream;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getRulesFile() {
        return rulesFile;
    }

    public void setRulesFile(String rulesFile) {
        this.rulesFile = rulesFile;
    }

    public int getRulesReloadSeconds() {
        return rulesReloadSeconds;
    }

    public void setRulesReloadSeconds(int rulesReloadSeconds) {
        this.rulesReloadSeconds = rulesReloadSeconds;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public long getDecisionStreamMaxLen() {
        return decisionStreamMaxLen;
    }

    public void setDecisionStreamMaxLen(long decisionStreamMaxLen) {
        this.decisionStreamMaxLen = decisionStreamMaxLen;
    }
}
