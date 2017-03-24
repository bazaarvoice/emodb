package com.bazaarvoice.emodb.databus.tracer;

import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.core.UpdateRefSerializer;
import com.bazaarvoice.emodb.event.tracer.CSVEventTracer;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.google.common.collect.ImmutableList;

import java.io.Writer;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.List;

/**
 * Extension of {@link CSVEventTracer} for writing databus events.  Columns are, by position:
 *
 * <ol>
 *     <li>Original document change ID</li>
 *     <li>Document change timestamp</li>
 *     <li>table</li>
 *     <li>key</li>
 *     <li>source</li>
 * </ol>
 */
public class DatabusCSVEventTracer extends CSVEventTracer {

    private final DateFormat _dateFormat = ISO8601DateFormat.getInstance();

    public DatabusCSVEventTracer(Writer out, boolean closeWriterOnClose) {
        super(out, closeWriterOnClose);
    }

    @Override
    protected List<Object> toColumns(String source, ByteBuffer data) {
        UpdateRef ref = UpdateRefSerializer.fromByteBuffer(data);
        return ImmutableList.of(
                ref.getChangeId(),
                _dateFormat.format(TimeUUIDs.getDate(ref.getChangeId())),
                ref.getTable(),
                ref.getKey(),
                source);
    }
}
