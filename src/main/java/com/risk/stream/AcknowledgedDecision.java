package com.risk.stream;

import com.risk.model.Decision;
import redis.clients.jedis.StreamEntryID;

import java.io.Serializable;

/**
 * 引擎输出：决策 + 对应交易流条目 ID（Sink 写完决策后用它 XACK）。
 * 标准 POJO，供 Flink 在算子间序列化。
 */
public class AcknowledgedDecision implements Serializable {

    private static final long serialVersionUID = 1L;

    private StreamEntryID sourceEntryId;
    private Decision decision;

    public AcknowledgedDecision() {
    }

    public AcknowledgedDecision(StreamEntryID sourceEntryId, Decision decision) {
        this.sourceEntryId = sourceEntryId;
        this.decision = decision;
    }

    public StreamEntryID getSourceEntryId() {
        return sourceEntryId;
    }

    public void setSourceEntryId(StreamEntryID sourceEntryId) {
        this.sourceEntryId = sourceEntryId;
    }

    public Decision getDecision() {
        return decision;
    }

    public void setDecision(Decision decision) {
        this.decision = decision;
    }
}
