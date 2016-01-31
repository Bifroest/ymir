package io.bifroest.ymir.cassandra.statistics;

import io.bifroest.retentions.RetentionTable;

public class CreateTableEvent {

    private final long timestamp;
    private final RetentionTable table;

    public CreateTableEvent( long timestamp, RetentionTable table ) {
        this.timestamp = timestamp;
        this.table = table;
    }

    public long timestamp() {
        return timestamp;
    }

    public RetentionTable table() {
        return table;
    }

}
