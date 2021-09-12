package com.bazaarvoice.emodb.web.scanner.writer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.io.Closeables;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

abstract public class ShardWriter implements ScanDestinationWriter {
    private final EmptyCheckedOutputStream _out;
    private final ObjectMapper _mapper;
    private final JsonGenerator _jsonGenerator;

    ShardWriter(OutputStream out, ObjectMapper objectMapper) throws IOException {
        _out = new EmptyCheckedOutputStream(out);
        _mapper = objectMapper;
        _jsonGenerator = createGenerator();
    }

    private JsonGenerator createGenerator()
            throws IOException {
        JsonGenerator generator = _mapper.getFactory().createGenerator(_out);
        // Disable closing the output stream on completion
        generator.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        return generator;
    }

    public OutputStream getOutputStream() {
        return _out;
    }

    public void closeAndTransferAsync(Optional<Integer> finalPartCount) throws IOException {
        _jsonGenerator.close();
        _out.close();

        // GZIP output streams do not generate any output if no data was ever written.  In particular for
        // TemporaryFileScanWriter this means that no file is generated in the file system.  Therefore if shard
        // contained only deleted entries then the expected file would not exist.  To correctly handle this
        // circumstance explicitly pass to the the implementation whether any data should be expected.s
        ready(_out.isEmpty(), finalPartCount);
    }

    public void closeAndCancel() {
        try {
            Closeables.close(_jsonGenerator, true);
            Closeables.close(_out, true);
        } catch (IOException e) {
            // Won't happen, exception is swallowed
        }
        cancel();
    }

    abstract protected void ready(boolean isEmpty, Optional<Integer> finalPartCount) throws IOException;

    abstract protected void cancel();

    /**
     * OutputStream which wraps another OutputStream and keeps track of whether any data was ever written to it.
     * Similar to {@link com.google.common.io.CountingOutputStream} but lighter weight since the actual count
     * is not important.
     */
    private static class EmptyCheckedOutputStream extends FilterOutputStream {
        private boolean _isEmpty = true;

        private EmptyCheckedOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            _isEmpty = _isEmpty && len == 0;
            out.write(b, off, len);
        }

        @Override
        public void write(int b) throws IOException {
            _isEmpty = false;
            out.write(b);
        }

        public boolean isEmpty() {
            return _isEmpty;
        }
    }

    @Override
    public void writeDocument(Map<String, Object> document) throws IOException {
        _mapper.writeValue(_jsonGenerator, document);
        _jsonGenerator.writeRaw('\n');
    }
}
