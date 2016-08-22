package com.bazaarvoice.emodb.event.db.astyanax;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

public class SlabRef {
    private final String _channel;
    private final ByteBuffer _slabId;
    private final ManifestPersister _persister;
    private final AtomicInteger _refCount = new AtomicInteger(1);

    SlabRef(String channel, ByteBuffer slabId, ManifestPersister persister) {
        _channel = checkNotNull(channel);
        _slabId = checkNotNull(slabId);
        _persister = checkNotNull(persister);
        _persister.open(_channel, _slabId);
    }

    public ByteBuffer getSlabId() {
        return _slabId;
    }

    public SlabRef addRef() {
        _refCount.incrementAndGet();
        return this;
    }

    public void release() {
        if (_refCount.decrementAndGet() == 0) {
            _persister.close(_channel, _slabId);
        }
    }
}
