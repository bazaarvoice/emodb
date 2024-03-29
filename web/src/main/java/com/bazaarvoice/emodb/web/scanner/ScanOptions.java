package com.bazaarvoice.emodb.web.scanner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

/**
 * POJO to hold the options for how a scan and upload operation is configured.
 */
@JsonIgnoreProperties (ignoreUnknown = true)
public class ScanOptions {

    private final static int DEFAULT_MAX_CONCURRENT_SUB_RANGE_SCANS = 4;
    private final static int DEFAULT_RANGE_SCAN_SPLIT_SIZE = 1000000;
    private final static Duration DEFAULT_MAX_RANGE_SCAN_TIME = Duration.ofMinutes(10);

    private final Set<String> _placements;
    private final Set<ScanDestination> _destinations = Sets.newHashSet();
    // True to scan only one availability zone at a time, false to scan all token ranges concurrently
    private boolean _scanByAZ = false;
    // Maximum number of scans that can be performed on subranges of a single token range owned by a single host on the ring
    private int _maxConcurrentSubRangeScans = DEFAULT_MAX_CONCURRENT_SUB_RANGE_SCANS;
    // Maximum number of rows to scan in a single range scan task.
    private int _rangeScanSplitSize = DEFAULT_RANGE_SCAN_SPLIT_SIZE;
    // Maximum time a range scan can run before it is automatically stopped and remaining work split to a new task
    private Duration _maxRangeScanTime = DEFAULT_MAX_RANGE_SCAN_TIME;
    // Allow compaction of records during the scan.  Potentially increases the total scan time.  Default is false
    private boolean _compactionEnabled = false;
    // Whether or not to stash temporally
    private boolean _temporalEnabled = true;
    // Whether or not to only scan live ranges according to StashTableDAO
    private boolean _onlyScanLiveRanges = true;

    public ScanOptions(String placement) {
        this(ImmutableSortedSet.of(placement));
    }

    public ScanOptions(Collection<String> placements) {
        _placements = ImmutableSortedSet.copyOf(requireNonNull(placements, "placements"));
        checkArgument(!placements.isEmpty(), "At least one placement is required");
    }

    @JsonCreator
    private ScanOptions(@JsonProperty ("placements") List<String> placements,
                        @JsonProperty ("destinations") List<ScanDestination> destinations,
                        @JsonProperty ("scanByAZ") Boolean scanByAZ,
                        @JsonProperty ("maxConcurrentSubRangeScans") Integer maxConcurrentSubRangeScans,
                        @JsonProperty ("rangeScanSplitSize") Integer rangeScanSplitSize,
                        @JsonProperty ("maxRangeScanTime") Long maxRangeScanTime,
                        @JsonProperty ("compactionEnabled") Boolean compactionEnabled,
                        @JsonProperty ("temporalEnabled") Boolean temporalEnabled,
                        @JsonProperty ("onlyScanLiveRanges") Boolean onlyScanLiveRanges) {
        this(placements);
        if (destinations != null) {
            addDestinations(destinations);
        }
        if (scanByAZ != null) {
            _scanByAZ = scanByAZ;
        }
        if (maxConcurrentSubRangeScans != null) {
            _maxConcurrentSubRangeScans = maxConcurrentSubRangeScans;
        }
        if (rangeScanSplitSize != null) {
            _rangeScanSplitSize = rangeScanSplitSize;
        }
        if (maxRangeScanTime != null) {
            _maxRangeScanTime = Duration.ofMillis(maxRangeScanTime);
        }
        if (compactionEnabled != null) {
            _compactionEnabled = compactionEnabled;
        }
        if (temporalEnabled != null) {
            _temporalEnabled = temporalEnabled;
        }
        if (onlyScanLiveRanges != null) {
            _onlyScanLiveRanges = onlyScanLiveRanges;
        }
    }

    @JsonSerialize
    public Set<String> getPlacements() {
        return _placements;
    }

    public ScanOptions addDestination(ScanDestination destination) {
        _destinations.add(requireNonNull(destination, "destination"));
        return this;
    }

    public ScanOptions addDestinations(Collection<ScanDestination> destinations) {
        for (ScanDestination destination : destinations) {
            _destinations.add(requireNonNull(destination, "destination"));
        }
        return this;
    }

    public Set<ScanDestination> getDestinations() {
        return ImmutableSet.copyOf(_destinations);
    }

    public boolean isScanByAZ() {
        return _scanByAZ;
    }

    public ScanOptions setScanByAZ(boolean scanByAZ) {
        _scanByAZ = scanByAZ;
        return this;
    }

    public int getMaxConcurrentSubRangeScans() {
        return _maxConcurrentSubRangeScans;
    }

    public ScanOptions setMaxConcurrentSubRangeScans(int maxConcurrentSubRangeScans) {
        checkArgument(maxConcurrentSubRangeScans >= 1, "maxConcurrentSubRangeScans < 1");
        _maxConcurrentSubRangeScans = maxConcurrentSubRangeScans;
        return this;
    }

    public int getRangeScanSplitSize() {
        return _rangeScanSplitSize;
    }

    public ScanOptions setRangeScanSplitSize(int rangeScanSplitSize) {
        checkArgument(rangeScanSplitSize >= 1, "rangeScanSplitSize < 1");
        _rangeScanSplitSize = rangeScanSplitSize;
        return this;
    }

    @JsonProperty("maxRangeScanTime")
    public long getMaxRangeScanTimeMs() {
        return _maxRangeScanTime.toMillis();
    }

    @JsonIgnore
    public Duration getMaxRangeScanTime() {
        return _maxRangeScanTime;
    }

    public ScanOptions setMaxRangeScanTime(Duration maxRangeScanTime) {
        requireNonNull(maxRangeScanTime, "maxRangeScanTime");
        checkArgument(maxRangeScanTime.compareTo(Duration.ZERO) > 0, "Duration must not be empty");
        _maxRangeScanTime = maxRangeScanTime;
        return this;
    }

    public boolean isCompactionEnabled() {
        return _compactionEnabled;
    }

    public ScanOptions setCompactionEnabled(boolean compactionEnabled) {
        _compactionEnabled = compactionEnabled;
        return this;
    }

    public boolean isTemporalEnabled() {
        return _temporalEnabled;
    }

    public ScanOptions setTemporalEnabled(boolean temporalEnabled) {
        _temporalEnabled = temporalEnabled;
        return this;
    }

    public boolean isOnlyScanLiveRanges() {
        return _onlyScanLiveRanges;
    }

    public ScanOptions setOnlyScanLiveRanges(boolean onlyScanLiveRanges) {
        _onlyScanLiveRanges = onlyScanLiveRanges;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ScanOptions)) {
            return false;
        }

        ScanOptions that = (ScanOptions) o;

        return Objects.equals(_placements, that.getPlacements()) &&
                _scanByAZ == that._scanByAZ &&
                _compactionEnabled == that._compactionEnabled &&
                _maxConcurrentSubRangeScans == that._maxConcurrentSubRangeScans &&
                Objects.equals(_destinations, that.getDestinations());
    }

    @Override
    public int hashCode() {
        return hash(_placements);
    }
}
