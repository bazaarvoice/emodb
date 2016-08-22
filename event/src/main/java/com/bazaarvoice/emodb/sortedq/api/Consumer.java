package com.bazaarvoice.emodb.sortedq.api;

import java.nio.ByteBuffer;
import java.util.List;

public interface Consumer {
    void consume(List<ByteBuffer> records);
}
