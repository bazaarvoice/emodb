package com.bazaarvoice.emodb.sor.db;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * POJO used to group splits in the following way:
 *
 * Groups:
 *    Each group contains a {@link TokenRange} list.  Each token range in a group does not share any replication servers
 *    with other token ranges in the group.  Therefore, performing parallel requests on the token ranges will not
 *    cause a hot spot on any one Cassandra server.
 *
 * TokenRanges:
 *     Each token range contains a {@link ScanRange} list.  Each scan range is a sub-range of the overall token range
 *     where each sub-range contains approximately the same number of rows.  Unlike with groups, parallel requests on a
 *     single token range's scan ranges will increase the resource utilization of the underlying Cassandra servers.
 *
 */
public class ScanRangeSplits {

    private final List<SplitGroup> _splitGroups;

    public ScanRangeSplits(List<SplitGroup> splitGroups) {
        _splitGroups = requireNonNull(splitGroups, "splitGroups");
    }

    public List<SplitGroup> getSplitGroups() {
        return _splitGroups;
    }

    /**
     * Returns a new ScanRangeSplits where all of the token ranges from all groups are combined into a single group.
     * This is useful when the caller is going to perform an operation on all token ranges and is not concerned about
     * creating hot spots.
     */
    public ScanRangeSplits combineGroups() {
        return new ScanRangeSplits(ImmutableList.of(new SplitGroup(
                FluentIterable.from(_splitGroups)
                        .transformAndConcat(new Function<SplitGroup, Iterable<TokenRange>>() {
                            @Override
                            public Iterable<TokenRange> apply(SplitGroup splitGroup) {
                                return splitGroup.getTokenRanges();
                            }
                        })
                        .toList())));
    }

    /**
     * The simplest way to create a ScanRangeSplits is by starting here with a builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** POJO for providing the token ranges associated with a split group. */
    public static class SplitGroup {
        private final List<TokenRange> _tokenRanges;

        public SplitGroup(List<TokenRange> tokenRanges) {
            _tokenRanges = tokenRanges;
        }

        public List<TokenRange> getTokenRanges() {
            return _tokenRanges;
        }
    }

    /** POJO for providing the scan ranges associated with a token range. */
    public static class TokenRange {
        private final List<ScanRange> _scanRanges;

        public TokenRange(List<ScanRange> scanRanges) {
            _scanRanges = scanRanges;
        }

        public List<ScanRange> getScanRanges() {
            return _scanRanges;
        }
    }

    /**
     * Builder for ScanRangeSplits.  Typical use looks something like this:
     *
     * <code>
     * ScanRangeSplits.Builder builder = ScanRangeSplits.builder();
     * builder.addScanRange("1a", "firstKeyForTokenRange1", firstScanRangeForTokenRange1);
     * builder.addScanRange("1a", "firstKeyForTokenRange1", secondScanRangeForTokenRange1);
     * // other scan ranges for token range 1
     * builder.addScanRange("1a", "firstKeyForTokenRange1", finalScanRangeForTokenRange1);
     * // remaining scan ranges for all other token ranges
     * ScanRangeSplits splits = builder.build();
     *
     * </code>
     */
    public static class Builder {
        private final Table<String, String, ImmutableList.Builder<ScanRange>> _scanRangesByGroupAndAndTokenRange = HashBasedTable.create();

        private Builder() {
            // empty
        }

        /**
         * Adds a scan range for a particular token range in a group.  Typically the group corresponds to a Cassandra rack
         * (or availability zone in Amazon) and the token range corresponds to the a unique identifier for the physical
         * server which "owns" the token range in the rack.  The actual IDs are unimportant so long as the same value
         * is consistently used for each group and token range.
         */
        public Builder addScanRange(String groupId, String tokenRangeId, ScanRange scanRange) {
            ImmutableList.Builder<ScanRange> tokenRangeScanRanges = _scanRangesByGroupAndAndTokenRange.get(groupId, tokenRangeId);
            if (tokenRangeScanRanges == null) {
                tokenRangeScanRanges = ImmutableList.builder();
                _scanRangesByGroupAndAndTokenRange.put(groupId, tokenRangeId, tokenRangeScanRanges);
            }
            tokenRangeScanRanges.add(scanRange);
            return this;
        }

        /** Builds a ScanRangeSplits from this builder's state. */
        public ScanRangeSplits build() {
            ImmutableList.Builder<SplitGroup> splitGroups = ImmutableList.builder();

            for (Map.Entry<String, Map<String, ImmutableList.Builder<ScanRange>>> splitGroupRow : _scanRangesByGroupAndAndTokenRange.rowMap().entrySet()) {
                ImmutableList.Builder<TokenRange> tokenRanges = ImmutableList.builder();

                for (ImmutableList.Builder<ScanRange> scanRange : splitGroupRow.getValue().values()) {
                    tokenRanges.add(new TokenRange(scanRange.build()));
                }

                splitGroups.add(new SplitGroup(tokenRanges.build()));
            }

            return new ScanRangeSplits(splitGroups.build());
        }
    }
}
