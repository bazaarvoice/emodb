package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.nio.BufferUtils;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.json.deferred.LazyJsonMap;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.db.LazyDelta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.deser.JsonTokener;
import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import org.apache.cassandra.utils.ByteBufferUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

class DefaultChangeEncoder implements ChangeEncoder {

    private enum Encoding {
        D1,  // delta (version 1 encoding)
        D2,  // delta (version 2 encoding includes tags)
        D3,  // delta (version 3 encoding, adds change flags to D2
        C1,  // compaction (version 1 encoding)
        A1,  // audit (version 1 encoding)
        H1,  // historical deltas (version 1 encoding)
    }

    private final Encoding _deltaEncoding;

    public DefaultChangeEncoder() {
        // Default constructor uses the latest version.
        this(3);
    }

    public DefaultChangeEncoder(int deltaEncodingVersion) {
        // To support a rolling upgrade between delta encodings the caller can specify which of the two most recent
        // delta encoding versions to use.  When upgrading the version should be deployed as the old version so that
        // old instances can read deltas written by the new instances.  Once all old instances have been terminated
        // the encoding version can be flipped to the new version.

        checkArgument(deltaEncodingVersion == 2 || deltaEncodingVersion == 3, "Only delta encoding versions 2 and 3 are permitted");
        _deltaEncoding = deltaEncodingVersion == 2 ? Encoding.D2 : Encoding.D3;
    }

    @Override
    public StringBuilder encodeDelta(String deltaString, @Nullable EnumSet<ChangeFlag> changeFlags, @Nonnull Set<String> tags, StringBuilder changeBody) {
        // Encoding will be either the legacy D2 or the current D3
        // Spec for D2 is <tags>:<delta>
        // Spec for D3 is <tags>:<change flags>:<Delta>

        changeBody.append(_deltaEncoding)
                .append(":")
                .append(tags.isEmpty() ? "[]" : JsonHelper.asJson(tags));

        if (_deltaEncoding == Encoding.D3) {
            changeBody.append(":");
            if (changeFlags != null) {
                for (ChangeFlag changeFlag : changeFlags) {
                    changeBody.append(changeFlag.serialize());
                }
            }
        }

        changeBody.append(":")
                .append(deltaString);


        return changeBody;
    }

    @Override
    public String encodeAudit(Audit audit) {
        return encodeChange(Encoding.A1, JsonHelper.asJson(audit), new StringBuilder()).toString();
    }

    @Override
    public StringBuilder encodeCompaction(Compaction compaction, StringBuilder prefix) {
        return encodeChange(Encoding.C1, JsonHelper.asJson(compaction), prefix);
    }

    @Override
    public String encodeHistory(History history) {
        return encodeChange(Encoding.H1, JsonHelper.asJson(history), new StringBuilder()).toString();
    }

    private StringBuilder encodeChange(Encoding encoding, String bodyString, StringBuilder prefix) {
        return prefix.append(encoding).append(":").append(bodyString);
    }

    /**
     * Decodes a change encoded by {@link #encodeChange}.
     */
    @Override
    public Change decodeChange(UUID changeId, ByteBuffer buf) {
        int sep = getSeparatorIndex(buf);
        Encoding encoding = getEncoding(buf, sep);
        String body = getBody(buf, sep);
        JsonTokener tokener;
        Set<String> tags;

        ChangeBuilder builder = new ChangeBuilder(changeId);
        switch (encoding) {
            case D1:
                builder.with(Deltas.fromString(body));
                break;
            case D2:
                // Spec for D2 is as follows:
                // D2:<tags>:<Delta>
                tokener = new JsonTokener(body);
                tags = FluentIterable.from(tokener.nextArray()).transform(Functions.toStringFunction()).toSet();
                tokener.next(':');
                builder.with(Deltas.fromString(tokener)).with(tags);
                break;
            case D3:
                // Spec for D3 is as follows:
                // D3:<tags>:<change flags>:<Delta>
                tokener = new JsonTokener(body);
                tags = FluentIterable.from(tokener.nextArray()).transform(Functions.toStringFunction()).toSet();
                tokener.next(':');
                boolean isConstant = false;
                boolean isMapDelta = false;
                char changeFlag = tokener.next();
                while (changeFlag != ':' && changeFlag != 0) {
                    // In the future there may be more change flags, though for now we're only interested in
                    // those provided below
                    switch (ChangeFlag.deserialize(changeFlag)) {
                        case CONSTANT_DELTA:
                            isConstant = true;
                            break;
                        case MAP_DELTA:
                            isMapDelta = true;
                            break;
                    }
                    changeFlag = tokener.next();
                }

                // There are numerous circumstances where the expense of parsing a literal map delta is wasted.  For
                // example, with two consecutive literal deltas for the same record the elder is immediately replaced
                // by the latter, so resources spent parsing and instantiating the elder are unnecessary.  Return a lazy
                // map literal instead to defer instantiation until necessary.
                if (isConstant && isMapDelta) {
                    builder.with(Deltas.literal(new LazyJsonMap(body.substring(tokener.pos())))).with(tags);
                } else {
                    // Even if the delta is not a literal map delta there are still benefits to evaluating it lazily.
                    // For example, if a delta is behind a compaction record but has not yet been deleted it won't
                    // be used.
                    builder.with(new LazyDelta(tokener, isConstant)).with(tags);
                }
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
        int position = buf.position();

        if (sep == 2) {
            switch (buf.get(position) | (buf.get(position + 1) << 8)) {
                case 'D' | ('1' << 8):
                    return Encoding.D1;
                case 'D' | ('2' << 8):
                    return Encoding.D2;
                case 'D' | ('3' << 8):
                    return Encoding.D3;
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