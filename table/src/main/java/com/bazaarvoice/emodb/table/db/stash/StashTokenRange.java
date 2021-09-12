package com.bazaarvoice.emodb.table.db.stash;

import com.bazaarvoice.emodb.table.db.Table;

import java.nio.ByteBuffer;

/**
 * POJO for the association of a Cassanra token range with a table for a Stash snapshot.
 * @see com.bazaarvoice.emodb.table.db.StashTableDAO#getStashTokenRangesFromSnapshot(String, String, ByteBuffer, ByteBuffer) 
 */
public class StashTokenRange {
    private final ByteBuffer _from;
    private final ByteBuffer _to;
    private final Table _table;

    public StashTokenRange(ByteBuffer from, ByteBuffer to, Table table) {
        _from = from;
        _to = to;
        _table = table;
    }

    public ByteBuffer getFrom() {
        return _from;
    }

    public ByteBuffer getTo() {
        return _to;
    }

    public Table getTable() {
        return _table;
    }
}
