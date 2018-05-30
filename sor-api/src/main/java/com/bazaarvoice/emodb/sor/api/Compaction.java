package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.common.json.deferred.LazyJsonMap;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaParser;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Placeholder for adjacent deltas that have been consolidated into a single
 * update to achieve a more compact representation in the data store.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Compaction {

    /** Number of old consistent deltas deleted by compaction. */
    private final long _count;

    /** UUID of the oldest consistent delta deleted by compaction. */
    private final UUID _first;

    /** UUID of the oldest consistent delta not deleted by compaction. */
    private final UUID _cutoff;

    /** The 'signature' intrinsic field of the object as of the cutoff UUID. */
    private final String _cutoffSignature;

    /** This holds the compacted delta, instead of referring to a cutoff delta. */
    private Delta _compactedDelta;

    private Set<String> _lastTags;

    /**
     * UUID of the most recent consistent delta which changed the resolved object deleted by compaction.  Any delta
     * between this and the cutoff resolve to the same content, excluding intrinsics such as version and signature.
     */
     private final UUID _lastContentMutation;

    /**
     * Like {@link #_lastContentMutation} except this is the UUID of the most consistent delta which changed the
     * resolved object or its metadata (namely, the event tags).  Any delta between this and the cutoff do not need to
     * be posted to the databus because they are redundant to the subscriber.  For example, assume an update is made
     * at time t1 with tags ["alpha"].  Later, at time t2 a no-op update <code>..</code> is made with tags ["beta"].
     * This second update would change <code>_lastMutation</code> but not <code>_lastContentMutation</code>.  Finally,
     * a third no-op delta at time t3 with the same tags as the previous update, ["beta"], would change neither
     * <code>_lastMutation</code> nor <code>_lastContentMutation</code>.
     */
    private final UUID _lastMutation;

    public Compaction(long count,
                      @Nullable UUID first,
                      @Nullable UUID cutoff,
                      @Nullable String cutoffSignature,
                      @Nullable UUID lastContentMutation,
                      @Nullable UUID lastMutation) {
        this(count, first, cutoff, cutoffSignature, lastContentMutation, lastMutation, (Delta) null, null);
    }

    public Compaction(long count,
                      @Nullable UUID first,
                      @Nullable UUID cutoff,
                      @Nullable String cutoffSignature,
                      @Nullable UUID lastContentMutation,
                      @Nullable UUID lastMutation,
                      Delta compactedDelta) {
        this(count, first, cutoff, cutoffSignature, lastContentMutation, lastMutation, compactedDelta, null);
    }

    public Compaction(long count,
                      @Nullable UUID first,
                      @Nullable UUID cutoff,
                      @Nullable String cutoffSignature,
                      @Nullable UUID lastContentMutation,
                      @Nullable UUID lastMutation,
                      @Nullable Delta compactedDelta,
                      @Nullable Set<String> lastTags) {
        this(count, first, cutoff, cutoffSignature, lastContentMutation, lastMutation, lastTags);
        _compactedDelta = compactedDelta;
    }

    /**
     * Compactions are most commonly deserialized when a record contains one or more compaction records.  If there
     * is more than one all but the algorithmically selected compaction record are ignored.  As an efficiency gain
     * the compacted delta may be lazily deserialized so that resolution only occurs on the selected record.
     */
    @JsonCreator
    private Compaction(@JsonProperty("count") long count,
                       @JsonProperty("first") @Nullable UUID first,
                       @JsonProperty("cutoff") @Nullable UUID cutoff,
                       @JsonProperty("cutoffSignature") @Nullable String cutoffSignature,
                       @JsonProperty("lastContentMutation") @Nullable UUID lastContentMutation,
                       @JsonProperty("lastMutation") @Nullable UUID lastMutation,
                       @JsonProperty("compactedDelta") @Nullable String compactedDeltaString,
                       @JsonProperty("lastTags") @Nullable Set<String> lastTags) {
        this(count, first, cutoff, cutoffSignature, lastContentMutation, lastMutation, lastTags);

        if (compactedDeltaString != null) {
            // By definition the compacted delta must be constant.  If the literal is a map then there are many
            // circumstances where performance is improved by lazily converting the literal to a JSON map, such as when
            // the literal will be streamed as-is to an API request.  Therefore create a literal delta from a lazy map.

            // Shouldn't be any leading white space, but better to be careful.
            for (int i=0; i < compactedDeltaString.length(); i++) {
                if (!Character.isWhitespace(compactedDeltaString.charAt(i))) {
                    if (compactedDeltaString.charAt(i) == '{') {
                        _compactedDelta = Deltas.literal(new LazyJsonMap(compactedDeltaString));
                    } else {
                        _compactedDelta = DeltaParser.parse(compactedDeltaString);
                    }
                    break;
                }
            }
        }
    }

    public Compaction(long count,
                      @Nullable UUID first,
                      @Nullable UUID cutoff,
                      @Nullable String cutoffSignature,
                      @Nullable UUID lastContentMutation,
                      @Nullable UUID lastMutation,
                      @Nullable Set<String> lastTags) {

        if ((count == 0 && (first != null || cutoff != null)) ||
                (count > 0 && (first == null || cutoff == null)) ||
                ((cutoff == null) == (cutoffSignature != null))) {
            throw new IllegalArgumentException("Invalid initial state");
        }
        _count = count;
        _first = first;
        _cutoff = cutoff;
        _cutoffSignature = cutoffSignature;
        // For historical reasons there may exist grandfathered-in compactions where the last mutation was recorded
        // but not the last content mutation.  In these cases substitute the last mutation if available
        _lastContentMutation = lastContentMutation != null ? lastContentMutation : lastMutation;
        _lastMutation = lastMutation;
        _lastTags = lastTags == null ? Collections.emptySet() : lastTags;
    }


    public long getCount() {
        return _count;
    }

    public UUID getFirst() {
        return _first;
    }

    public UUID getLastContentMutation() {
        return _lastContentMutation;
    }

    public UUID getLastMutation() {
        return _lastMutation;
    }

    public UUID getCutoff() {
        return _cutoff;
    }

    public String getCutoffSignature() {
        return _cutoffSignature;
    }

    public Delta getCompactedDelta() {
        return _compactedDelta;
    }

    public boolean hasCompactedDelta() {
        return _compactedDelta != null;
    }

    /** Last applied tags */
    @Nullable
    public Set<String> getLastTags() {
        return _lastTags;
    }

    /** For backwards compatibility, we need to do this **/
    public Compaction setCompactedDelta(Delta delta) {
        _compactedDelta = delta;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Compaction)) {
            return false;
        }
        Compaction that = (Compaction) o;
        return _count == that._count &&
                Objects.equals(_first, that.getFirst()) &&
                Objects.equals(_cutoff, that.getCutoff()) &&
                Objects.equals(_cutoffSignature, that.getCutoffSignature()) &&
                Objects.equals(_lastMutation, that.getLastMutation()) &&
                Objects.equals(_lastContentMutation, that.getLastContentMutation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(_count, _first, _cutoff, _cutoffSignature, _lastMutation, _lastContentMutation);
    }
}
