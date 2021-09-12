package com.bazaarvoice.emodb.sor.db.astyanax;


import com.datastax.driver.core.*;

import java.nio.ByteBuffer;

public class StitchedRow extends AbstractGettableData implements Row {

    private final ByteBuffer _content;
    private final Row _oldRow;
    private final int _contentIndex;
    private final CodecRegistry _codecRegistry;
    private final int _numBlocks;

    public StitchedRow(ProtocolVersion protocolVersion, CodecRegistry codecRegistry, Row oldRow, ByteBuffer content, int contentIndex, int numBlocks) {
        super(protocolVersion);
        _codecRegistry = codecRegistry;
        _oldRow = oldRow;
        _content = content;
        _contentIndex = contentIndex;
        _numBlocks = numBlocks;
    }

    @Override
    protected int getIndexOf(String name) {
        return _oldRow.getColumnDefinitions().getIndexOf(name);
    }

    @Override
    protected DataType getType(int i) {
        return _oldRow.getColumnDefinitions().getType(i);
    }

    @Override
    protected String getName(int i) {
        return _oldRow.getColumnDefinitions().getName(i);
    }

    @Override
    protected ByteBuffer getValue(int i) {
        if (i == _contentIndex) {
            return _content;
        }
        return _oldRow.getBytesUnsafe(i);
    }

    @Override
    protected CodecRegistry getCodecRegistry() {
        return _codecRegistry;
    }

    @Override
    public Token getToken(int i) {
        return _oldRow.getToken(i);
    }

    @Override
    public Token getToken(String name) {
        return _oldRow.getToken(name);
    }

    @Override
    public Token getPartitionKeyToken() {
        return _oldRow.getPartitionKeyToken();
    }

    public int getNumBlocks() {
        return _numBlocks;
    }

    /**
     * This method cannot be overridden since constructing a ColumnDefinitions instance is private.  For our purposes
     * we don't need this anyway, so it throws UnsupportedOperationException
     */
    @Override
    public ColumnDefinitions getColumnDefinitions() {
        throw new UnsupportedOperationException("Cannot get column definitions for a stitched row");
    }
}