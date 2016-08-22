package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

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
     * UUID of the most recent consistent delta which changed the resolved object deleted by compaction.
     * Any delta between this and the cutoff are redundant.
     */
    private final UUID _lastMutation;

    public Compaction(long count,
                      @Nullable UUID first,
                      @Nullable UUID cutoff,
                      @Nullable String cutoffSignature,
                      @Nullable UUID lastMutation) {
        this(count, first, cutoff, cutoffSignature, lastMutation, null, null);
    }

    public Compaction(long count,
                      @Nullable UUID first,
                      @Nullable UUID cutoff,
                      @Nullable String cutoffSignature,
                      @Nullable UUID lastMutation, Delta compactedDelta) {
        this(count, first, cutoff, cutoffSignature, lastMutation, compactedDelta, null);
    }

    @JsonCreator
    public Compaction(@JsonProperty("count") long count,
                      @JsonProperty("first") @Nullable UUID first,
                      @JsonProperty("cutoff") @Nullable UUID cutoff,
                      @JsonProperty("cutoffSignature") @Nullable String cutoffSignature,
                      @JsonProperty("lastMutation") @Nullable UUID lastMutation,
                      @JsonProperty("compactedDelta") @Nullable Delta compactedDelta,
                      @JsonProperty("lastTags") @Nullable Set<String> lastTags) {
        checkArgument((count > 0) || (first == null && cutoff == null));
        checkArgument((count == 0) || (first != null && cutoff != null));
        checkArgument((cutoff != null) == (cutoffSignature != null));
        _count = count;
        _first = first;
        _cutoff = cutoff;
        _cutoffSignature = cutoffSignature;
        _lastMutation = lastMutation;
        _compactedDelta = compactedDelta;
        _lastTags = lastTags == null ? ImmutableSet.<String>of() : lastTags;
    }


    public long getCount() {
        return _count;
    }

    public UUID getFirst() {
        return _first;
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
                Objects.equal(_first, that.getFirst()) &&
                Objects.equal(_cutoff, that.getCutoff()) &&
                Objects.equal(_cutoffSignature, that.getCutoffSignature()) &&
                Objects.equal(_lastMutation, that.getLastMutation());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_count, _first, _cutoff, _cutoffSignature, _lastMutation);
    }
}
