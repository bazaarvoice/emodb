package com.bazaarvoice.emodb.queue.core;

import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.nio.ByteBuffer;

class MessageSerializer {

    private static final ObjectMapper JSON = new MappingJsonFactory().getCodec();

    static ByteBuffer toByteBuffer(Object json) {
        try {
            return ByteBuffer.wrap(JSON.writeValueAsBytes(json));
        } catch (IOException e) {
            // Shouldn't get I/O errors writing to a byte buffer.
            throw Throwables.propagate(e);
        }
    }

    static Object fromByteBuffer(ByteBuffer buf) {
        try {
            return JSON.readValue(new ByteBufferInputStream(buf.duplicate()), Object.class);
        } catch (IOException e) {
            // Must be malformed JSON.  Other kinds of I/O errors don't get thrown when reading from a string.
            throw new IllegalArgumentException(e.toString());
        }
    }
}
