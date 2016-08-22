package com.bazaarvoice.emodb.event.db.astyanax;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

import java.nio.ByteBuffer;

class ColumnFamilies {
    static ColumnFamily<String, ByteBuffer> MANIFEST = new ColumnFamily<>("manifest",
            StringSerializer.get(), ByteBufferSerializer.get());

    static ColumnFamily<ByteBuffer, Integer> SLAB = new ColumnFamily<>("slab",
            ByteBufferSerializer.get(), IntegerSerializer.get());
}
