package com.risk.stream;

import com.risk.model.TransactionEvent;
import redis.clients.jedis.StreamEntryID;

import java.io.Serializable;

/**
 * 从交易 Redis Stream 读到的一条消息：Stream 条目 ID（处理成功后用于 XACK）+ 解析后的事件。
 * 标准 POJO，供 Flink 在算子间序列化。
 */
public class StreamTransaction implements Serializable {

    private static final long serialVersionUID = 1L;

    private StreamEntryID entryId;
    private TransactionEvent event;

    public StreamTransaction() {
    }

    public StreamTransaction(StreamEntryID entryId, TransactionEvent event) {
        this.entryId = entryId;
        this.event = event;
    }

    public StreamEntryID getEntryId() {
        return entryId;
    }

    public void setEntryId(StreamEntryID entryId) {
        this.entryId = entryId;
    }

    public TransactionEvent getEvent() {
        return event;
    }

    public void setEvent(TransactionEvent event) {
        this.event = event;
    }
}
