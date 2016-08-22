package com.bazaarvoice.emodb.event.api;

import java.nio.ByteBuffer;
import java.util.List;

public interface ScanSink {
    void accept(List<ByteBuffer> events);
}
