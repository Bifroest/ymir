package com.goodgame.profiling.bifroest_ymir.cassandra.statistics;

import com.goodgame.profiling.graphite_retentions.RetentionTable;

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
