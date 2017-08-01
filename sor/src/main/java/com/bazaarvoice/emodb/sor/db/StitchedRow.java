package com.bazaarvoice.emodb.sor.db;


import com.datastax.driver.core.*;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class StitchedRow implements Row {

    private ByteBuffer _content;
    private Row _firstRow;
    int _contentIndex;

    public StitchedRow(List<Row> rows, int contentIndex, int contentSize) {
        _firstRow = rows.get(0);
        _contentIndex = contentIndex;

        ByteBuffer content = ByteBuffer.allocate(contentSize);
        int position = content.position();
        for (Row row : rows) {
            content.put(row.getBytesUnsafe(contentIndex));
        }
        content.position(position);
        _content = content;
    }

    @Override
    public int getInt(int i) {
        return _firstRow.getInt(i);
    }

    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        if (i == _contentIndex) {
            return _content;
        }
        return _firstRow.getBytesUnsafe(i);
    }

    @Override
    public String getString(int i) {
        if (i == _contentIndex) {
            byte[] bytes = new byte[_content.remaining()];
            _content.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        }
        return _firstRow.getString(i);
    }

    @Override
    public UUID getUUID(int i) {
        return _firstRow.getUUID(i);
    }

    @Override
    public UUID getUUID(String name) {
        return _firstRow.getUUID(name);
    }

    @Override
    public <T> T get(String name, TypeCodec<T> codec) {
        return null;
    }


    @Override
    public ColumnDefinitions getColumnDefinitions() {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public Token getToken(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public Token getToken(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public Token getPartitionKeyToken() {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public boolean isNull(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public boolean getBool(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");

    }

    @Override
    public byte getByte(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public short getShort(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public long getLong(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public Date getTimestamp(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public LocalDate getDate(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public long getTime(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public float getFloat(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public double getDouble(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public ByteBuffer getBytes(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public BigInteger getVarint(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public BigDecimal getDecimal(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public InetAddress getInet(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> List<T> getList(int i, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public UDTValue getUDTValue(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public TupleValue getTupleValue(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public Object getObject(int i) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> T get(int i, Class<T> targetClass) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> T get(int i, TypeToken<T> targetType) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> T get(int i, TypeCodec<T> codec) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public boolean isNull(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public boolean getBool(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public byte getByte(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public short getShort(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public int getInt(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public long getLong(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public Date getTimestamp(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public LocalDate getDate(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public long getTime(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public float getFloat(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public double getDouble(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public ByteBuffer getBytes(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public String getString(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public BigInteger getVarint(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public BigDecimal getDecimal(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public InetAddress getInet(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> List<T> getList(String name, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> Set<T> getSet(String name, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public UDTValue getUDTValue(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public TupleValue getTupleValue(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public Object getObject(String name) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> T get(String name, Class<T> targetClass) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

    @Override
    public <T> T get(String name, TypeToken<T> targetType) {
        throw new UnsupportedOperationException("This method is unimplemented");
    }

}