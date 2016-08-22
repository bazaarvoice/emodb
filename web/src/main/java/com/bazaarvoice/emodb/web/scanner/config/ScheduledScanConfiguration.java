package com.bazaarvoice.emodb.web.scanner.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Configuration for an optional scan and upload task that will run daily.
 */
public class ScheduledScanConfiguration {

    private final String DEFAULT_DAILY_SCAN_ID = "'daily'-yyyy-MM-dd-HH-mm-ss";

    // Time of day to perform a daily scan in GMT.  Should be formatted in 24 hour time, "HH:mmZ"
    // Leave unset to not perform an automated daily scan
    @Valid
    @NotNull
    @JsonProperty ("dailyScanTime")
    private Optional<String> _dailyScanTime = Optional.absent();

    // SimpleDateFormat string which defines each daily scan's ID.  Required iff dailyScanTime is set.
    @Valid
    @NotNull
    @JsonProperty ("scanId")
    private Optional<String> _scanId = Optional.of(DEFAULT_DAILY_SCAN_ID);

    // SimpleDateFormat string which defines the name of each daily scan's directory.  Required iff dailyScanTime
    // is set.  Default is intentionally not ISO8601 to avoid characters which interfere with Hadoop distributed copy.
    @Valid
    @NotNull
    @JsonProperty ("scanDirectory")
    private Optional<String> _scanDirectory = Optional.absent();

    // List of placements to scan and upload in the daily scan.  Required iff dailyScanTime is set.
    @Valid
    @NotNull
    @JsonProperty ("placements")
    private List<String> _placements = ImmutableList.of();

    // Maximum number of scans within a single range that can take place concurrently.  Required iff dailyScanTime is set.
    @Valid
    @NotNull
    @JsonProperty ("maxRangeConcurrency")
    private Optional<Integer> _maxRangeConcurrency = Optional.of(4);

    // Controls whether the daily scan should scan by availability zone.  This is slower but gives greater control
    // over the Cassandra load caused by the scan process.  Required iff defaultScanTime is set.  Default is true.
    @Valid
    @NotNull
    @JsonProperty ("scanByAZ")
    private Optional<Boolean> _scanByAZ = Optional.of(true);

    public Optional<String> getDailyScanTime() {
        return _dailyScanTime;
    }

    public ScheduledScanConfiguration setDailyScanTime(Optional<String> dailyScanTime) {
        _dailyScanTime = dailyScanTime;
        return this;
    }

    public Optional<String> getScanId() {
        return _scanId;
    }

    public ScheduledScanConfiguration setScanId(Optional<String> scanId) {
        _scanId = scanId;
        return this;
    }

    public Optional<String> getScanDirectory() {
        return _scanDirectory;
    }

    public ScheduledScanConfiguration setScanDirectory(Optional<String> scanDirectory) {
        _scanDirectory = scanDirectory;
        return this;
    }

    public List<String> getPlacements() {
        return _placements;
    }

    public ScheduledScanConfiguration setPlacements(List<String> placements) {
        _placements = placements;
        return this;
    }

    public Optional<Integer> getMaxRangeConcurrency() {
        return _maxRangeConcurrency;
    }

    public ScheduledScanConfiguration setMaxRangeConcurrency(Optional<Integer> maxRangeConcurrency) {
        _maxRangeConcurrency = maxRangeConcurrency;
        return this;
    }

    public Optional<Boolean> getScanByAZ() {
        return _scanByAZ;
    }

    public ScheduledScanConfiguration setScanByAZ(Optional<Boolean> scanByAZ) {
        _scanByAZ = scanByAZ;
        return this;
    }
}
