package com.bazaarvoice.emodb.event.db.astyanax;

import java.nio.ByteBuffer;
import java.util.Collection;

public interface ManifestPersister {
    void open(String channel, ByteBuffer slabId);

    void close(String channel, ByteBuffer slabId);

    void delete(String channel, ByteBuffer slabId);

    void move(String fromChannel, String toChannel, Collection<ByteBuffer> slabIds, boolean open);
}
