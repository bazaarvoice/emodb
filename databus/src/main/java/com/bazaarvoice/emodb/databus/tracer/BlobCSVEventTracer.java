package com.bazaarvoice.emodb.databus.tracer;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.databus.api.BlobCSVEventTracerSpec;
import com.bazaarvoice.emodb.event.api.EventTracer;
import com.bazaarvoice.emodb.event.tracer.CSVEventTracer;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link EventTracer} implementation which writes the results of a event tracer as a CSV in the blob store.
 * This works by writing the CSV to a local temporary file.  When the trace is closed the file is then copied to the
 * blob store and deleted.  The CSV file can optionally be gzipped and/or have a limited TTL.
 */
public class BlobCSVEventTracer implements EventTracer {

    private final Logger _log = LoggerFactory.getLogger(getClass());

    private final BlobStore _blobStore;
    private final String _table;
    private final String _blobId;
    private final boolean _gzipped;
    private final Duration _ttl;
    private File _tmpFile;
    private CSVEventTracer _csvEventTracer;

    public BlobCSVEventTracer(BlobStore blobStore, BlobCSVEventTracerSpec spec) {
        this(blobStore, spec.getTable(), spec.getBlobId(), spec.getTtl(), spec.isGzipped());
    }

    public BlobCSVEventTracer(BlobStore blobStore, String table, String blobId, @Nullable Duration ttl, boolean gzipped) {
        _blobStore = checkNotNull(blobStore, "blobStore");
        _table = checkNotNull(table, "table");
        _blobId = checkNotNull(blobId, "blobId");
        _ttl = ttl;
        _gzipped = gzipped;
    }

    @Override
    public void trace(String source, ByteBuffer data) {
        if (_tmpFile == null) {
            try {
                _tmpFile = File.createTempFile("emo", ".tmp");
                OutputStream out = new FileOutputStream(_tmpFile);
                if (_gzipped) {
                    out = new GZIPOutputStream(out);
                }
                _csvEventTracer = new DatabusCSVEventTracer(new OutputStreamWriter(out, Charsets.UTF_8), true);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        _csvEventTracer.trace(source, data);
    }

    @Override
    public void close() throws IOException {
        if (_tmpFile != null) {
            try {
                _csvEventTracer.close();
                _blobStore.put(_table, _blobId, Files.asByteSource(_tmpFile), ImmutableMap.of(), _ttl);
            } catch (UnknownTableException e) {
                // For this specific exception log the problem locally; don't pass up to the caller.
                _log.warn("Blob trace attempted to store results in unknown table: {}", _table);
            } catch(Exception e){
                Throwables.propagateIfInstanceOf(e, IOException.class);
                throw Throwables.propagate(e);
            } finally{
                if (_tmpFile != null) {
                    _tmpFile.delete();
                }
            }
        }
    }
}
