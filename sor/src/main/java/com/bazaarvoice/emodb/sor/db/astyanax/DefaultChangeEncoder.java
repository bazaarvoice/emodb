package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.nio.BufferUtils;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.deser.JsonTokener;
import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import org.apache.cassandra.utils.ByteBufferUtil;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

class DefaultChangeEncoder implements ChangeEncoder {
    private enum Encoding {
        D1,  // delta (version 1 encoding)
        D2,  // delta (version 2 encoding includes tags)
        C1,  // compaction (version 1 encoding)
        A1,  // audit (version 1 encoding)
        H1,  // historical deltas (version 1 encoding)
    }

    @Override
    public String encodeDelta(String deltaString, @Nonnull Set<String> tags) {
        // Spec for D2 is <tags>:<delta>
        String changeBody = (tags.isEmpty() ? "[]" : JsonHelper.asJson(tags)) + ":" + deltaString;
        return encodeChange(Encoding.D2, changeBody);
    }

    @Override
    public String encodeAudit(Audit audit) {
        return encodeChange(Encoding.A1, JsonHelper.asJson(audit));
    }

    @Override
    public String encodeCompaction(Compaction compaction) {
        return encodeChange(Encoding.C1, JsonHelper.asJson(compaction));
    }

    @Override
    public String encodeHistory(History history) {
        return encodeChange(Encoding.H1, JsonHelper.asJson(history));
    }

    private String encodeChange(Encoding encoding, String bodyString) {
        return encoding + ":" + bodyString;
    }

    /**
     * Decodes a change encoded by {@link #encodeChange}.
     */
    @Override
    public Change decodeChange(UUID changeId, ByteBuffer buf) {
        int sep = getSeparatorIndex(buf);
        Encoding encoding = getEncoding(buf, sep);
        String body = getBody(buf, sep);

        ChangeBuilder builder = new ChangeBuilder(changeId);
        switch (encoding) {
            case D1:
                builder.with(Deltas.fromString(body));
                break;
            case D2:
                // Spec for D2 is as follows:
                // D2:<tags>:<Delta>
                JsonTokener tokener = new JsonTokener(body);
                Set<String> tags = FluentIterable.from(tokener.nextArray()).transform(Functions.toStringFunction()).toSet();
                tokener.next(':');
                builder.with(Deltas.fromString(tokener)).with(tags);
                break;
            case C1:
                builder.with(JsonHelper.fromJson(body, Compaction.class));
                break;
            case A1:
                builder.with(JsonHelper.fromJson(body, Audit.class));
                break;
            case H1:
                builder.with(JsonHelper.fromJson(body, History.class));
                break;
            default:
                throw new UnsupportedOperationException(encoding.name());
        }
        return builder.build();
    }

    @Override
    public Compaction decodeCompaction(ByteBuffer buf) {
        // Used in the first pass of the resolver, doesn't bother decoding deltas since they're not relevant in pass 1.
        int sep = getSeparatorIndex(buf);
        if (getEncoding(buf, sep) != Encoding.C1) {
            return null;  // Not a compaction record
        }
        return JsonHelper.fromJson(getBody(buf, sep), Compaction.class);
    }

    /** Returns the index of the colon that separates the encoding prefix from the body suffix. */
    private int getSeparatorIndex(ByteBuffer buf) {
        int colon = BufferUtils.indexOf(buf, (byte) ':');
        if (colon == -1) {
            throw new IllegalStateException("Unknown encoding format: " + ByteBufferUtil.bytesToHex(buf));
        }
        return colon;
    }

    private Encoding getEncoding(ByteBuffer buf, int sep) {
        // This method gets called frequently enough that it's worth avoiding string building & memory allocation.
        if (sep == 2) {
            switch (buf.get(0) | (buf.get(1) << 8)) {
                case 'D' | ('1' << 8):
                    return Encoding.D1;
                case 'D' | ('2' << 8):
                    return Encoding.D2;
                case 'C' | ('1' << 8):
                    return Encoding.C1;
                case 'A' | ('1' << 8):
                    return Encoding.A1;
                case 'H' | ('1' << 8):
                    return Encoding.H1;
            }
        }
        throw new IllegalArgumentException("Unknown encoding: " + BufferUtils.getString(buf, 0, sep, Charsets.US_ASCII));
    }

    private String getBody(ByteBuffer buf, int sep) {
        return BufferUtils.getString(buf, sep + 1, buf.remaining() - (sep + 1), Charsets.UTF_8);
    }
}
