package com.bazaarvoice.emodb.web.scanner.writer;

import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricCounterOutputStream;
import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.bazaarvoice.emodb.common.stash.StashUtil;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Useful base implementation for ScanWriter, including:
 * <p>
 * 1. Converting base files to URIs
 * 2. Configuring file compression
 * 3. Providing counter metrics for the number of bytes written
 */
abstract public class AbstractScanWriter implements ScanWriter {

    private final Logger _log = LoggerFactory.getLogger(getClass());

    private final URI _baseUri;
    private final String _type;
    protected final Compression _compression;
    protected final int _taskId;
    private final MetricRegistry _metricRegistry;
    protected volatile boolean _closed = false;

    protected AbstractScanWriter(String type, int taskId, URI baseUri, Compression compression,
                                 MetricRegistry metricRegistry) {
        _type = requireNonNull(type, "type");
        _taskId = taskId;
        _baseUri = requireNonNull(baseUri, "baseUri");
        _compression = requireNonNull(compression, "compression");
        _metricRegistry = requireNonNull(metricRegistry, "metricRegistry");
    }

    protected URI getUriForShard(String tableName, int shardId, long tableUuid) {
        String sanitizedTableName = StashUtil.encodeStashTable(tableName);
        return UriBuilder.fromUri(_baseUri)
                .path(sanitizedTableName)
                .path(format("%s-%02x-%016x-%d.json%s", sanitizedTableName, shardId, tableUuid, _taskId, _compression.getExtension()))
                .build();
    }

    protected Counter getCounterForPlacement(String placement) {
        return _metricRegistry.counter(
                MetricRegistry.name("bv.emodb.scan.ScanUploader.placement", placement, _type + "-bytes-uploaded"));
    }

    protected OutputStream open(File file, @Nullable Counter counter) throws IOException {
        OutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
        if (counter != null) {
            stream = new MetricCounterOutputStream(stream, counter);
        }

        switch (_compression) {
            case GZIP:
                stream = new GZIPOutputStream(stream);
                break;
            case SNAPPY:
                stream = new SnappyOutputStream(stream);
                break;
        }

        return stream;
    }

    protected InputStream getInputStream(File file)
            throws IOException {
        InputStream in = new FileInputStream(file);
        switch (_compression) {
            case GZIP:
                in = new GZIPInputStream(in);
                break;
            case SNAPPY:
                in = new SnappyInputStream(in);
                break;
        }
        return in;
    }

    protected File concatenateFiles(Collection<File> sourceFiles, File dest)
            throws IOException {
        if (sourceFiles.size() == 1) {
            // Only one file, just copy it to the destination
            File src = sourceFiles.iterator().next();
            Files.copy(src, dest);
        } else if (_compression == Compression.GZIP) {
            // A handy attribute of GZIP files is that concatenating multiple GZIP files yields a valid GZIP file
            try (FileOutputStream out = new FileOutputStream(dest)) {
                for (File src : sourceFiles) {
                    Files.copy(src, out);
                }
            }
        } else {
            // Perform the slower uncompress-and-recompress operation
            try (OutputStream out = open(dest, null)) {
                for (File src : sourceFiles) {
                    try (InputStream in = getInputStream(src)) {
                        ByteStreams.copy(in, out);
                    }
                }
            }
        }
        return dest;
    }

    @Override
    public boolean writeScanComplete(String scanId, Date startTime)
            throws IOException {
        // Write the start time, complete time and scan ID in a success file
        String contents = format("%s\n%s\n%s", new ISO8601DateFormat().format(startTime),
                new ISO8601DateFormat().format(new Date()), scanId);
        URI scanCompleteFileUri = UriBuilder.fromUri(_baseUri)
                .path(StashUtil.SUCCESS_FILE)
                .build();
        boolean complete = writeScanCompleteFile(scanCompleteFileUri, contents.getBytes(Charsets.UTF_8));

        if (complete) {
            // Cannot write a current file if the base URI is a root directory
            String path = Optional.ofNullable(_baseUri.getPath()).orElse("/");
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            if (!path.isEmpty()) {
                int idx = path.lastIndexOf('/');
                String parentPath = idx != -1 ? path.substring(0, idx) : "/";
                String scanPathName = idx != -1 ? path.substring(idx + 1) : path;

                // Write the name of the sub-path in the latest file
                URI latestFileUri = UriBuilder.fromUri(_baseUri)
                        .replacePath(parentPath)
                        .path(StashUtil.LATEST_FILE)
                        .build();

                try {
                    writeLatestFile(latestFileUri, scanPathName.getBytes(Charsets.UTF_8));
                } catch (Exception e) {
                    _log.warn("Failed to update latest file for scan {}", scanId, e);
                }
            }
        }

        return complete;
    }

    /**
     * Writes the contents to the "scan complete" file located at "fileUri" only if the file doesn't already exist.
     *
     * @return true if the file was written, false if the file already existed
     */
    abstract protected boolean writeScanCompleteFile(URI fileUri, byte[] contents)
            throws IOException;

    /**
     * Writes the contents to the "latest" file located at "fileUri".
     */
    abstract protected void writeLatestFile(URI fileUri, byte[] contents)
            throws IOException;

    public void close() {
        _closed = true;
    }
}
