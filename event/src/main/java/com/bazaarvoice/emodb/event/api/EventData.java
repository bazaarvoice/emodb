package com.bazaarvoice.emodb.event.api;

import java.nio.ByteBuffer;

public interface EventData {

    String getId();

    ByteBuffer getData();
}
