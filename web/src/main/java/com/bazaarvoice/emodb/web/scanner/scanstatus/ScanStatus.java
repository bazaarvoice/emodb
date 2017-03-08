package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.plugin.stash.StashMetadata;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.time.DateUtils;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * POJO which maintains the metadata and status for a scan and upload operation.
 */
public class ScanStatus {

    private final String _scanId;
    private final ScanOptions _options;
    private final boolean _canceled;
    private final List<ScanRangeStatus> _pendingScanRanges;
    private final List<ScanRangeStatus> _activeScanRanges;
    private final List<ScanRangeStatus> _completeScanRanges;
    private final Date _startTime;
    private Date _completeTime;
    private final Date _compactionControlTime;

    public ScanStatus(String scanId, ScanOptions options, boolean canceled,
                      Date startTime,
                      List<ScanRangeStatus> pendingScanRanges,
                      List<ScanRangeStatus> activeScanRanges,
                      List<ScanRangeStatus> completeScanRanges) {
        this(scanId, options, canceled, startTime, pendingScanRanges, activeScanRanges, completeScanRanges, null);
    }

    @JsonCreator
    public ScanStatus(@JsonProperty ("scanId") String scanId,
                      @JsonProperty ("options") ScanOptions options,
                      @JsonProperty ("canceled") boolean canceled,
                      @JsonProperty ("startTime") Date startTime,
                      @JsonProperty ("pendingScanRanges") List<ScanRangeStatus> pendingScanRanges,
                      @JsonProperty ("activeScanRanges") List<ScanRangeStatus> activeScanRanges,
                      @JsonProperty ("completeScanRanges") List<ScanRangeStatus> completeScanRanges,
                      @JsonProperty ("completeTime") @Nullable Date completeTime) {
        _scanId = scanId;
        _options = options;
        _canceled = canceled;
        _startTime = startTime;
        _pendingScanRanges = Objects.firstNonNull(pendingScanRanges, ImmutableList.<ScanRangeStatus>of());
        _activeScanRanges = Objects.firstNonNull(activeScanRanges, ImmutableList.<ScanRangeStatus>of());
        _completeScanRanges = Objects.firstNonNull(completeScanRanges, ImmutableList.<ScanRangeStatus>of());
        _completeTime = completeTime;
        // Adding 1 minute buffer time to the start time to give us the compaction control time. (setting the time in the future takes care of the issue of there being any in-flight compactions)
        _compactionControlTime = DateUtils.addMinutes(startTime, 1);
    }

    public String getScanId() {
        return _scanId;
    }

    public ScanOptions getOptions() {
        return _options;
    }

    public boolean isCanceled() {
        return _canceled;
    }

    public Date getStartTime() {
        return _startTime;
    }

    public List<ScanRangeStatus> getPendingScanRanges() {
        return _pendingScanRanges;
    }

    public List<ScanRangeStatus> getActiveScanRanges() {
        return _activeScanRanges;
    }

    public List<ScanRangeStatus> getCompleteScanRanges() {
        return _completeScanRanges;
    }

    @JsonIgnore
    public Iterable<ScanRangeStatus> getAllScanRanges() {
        return Iterables.concat(_pendingScanRanges, _activeScanRanges, _completeScanRanges);
    }

    @Nullable
    public Date getCompleteTime() {
        return _completeTime;
    }

    public void setCompleteTime(Date completeTime) {
        _completeTime = completeTime;
    }

    @JsonIgnore
    public Date getCompactionControlTime() {
        return _compactionControlTime;
    }

    @JsonIgnore
    public boolean isDone() {
        if (_canceled) {
            return true;
        }
        if (!_activeScanRanges.isEmpty() || !_pendingScanRanges.isEmpty()) {
            return false;
        }
        // If any completed scan required re-splitting then we're not done.
        for (ScanRangeStatus complete : _completeScanRanges) {
            if (complete.getResplitRange().isPresent()) {
                return false;
            }
        }
        return true;
    }

    public StashMetadata asPluginStashMetadata() {
        // Convert destinations to URIs
        Set<URI> destinations = Sets.newHashSet();
        for (ScanDestination destination : getOptions().getDestinations()) {
            if (destination.isDiscarding()) {
                // Replace a discarding destination with a URI to /dev/null
                destinations.add(URI.create("file:///dev/null"));
            } else {
                destinations.add(destination.getUri());
            }
        }
        return new StashMetadata(getScanId(), getStartTime(), getOptions().getPlacements(), destinations);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ScanStatus)) {
            return false;
        }

        ScanStatus that = (ScanStatus) o;

        return _scanId.equals(that._scanId) &&
                _canceled == that._canceled &&
                _startTime.equals(that._startTime) &&
                Objects.equal(_completeTime, that._completeTime) &&
                _options.equals(that._options) &&
                _activeScanRanges.equals(that._activeScanRanges) &&
                _completeScanRanges.equals(that._completeScanRanges) &&
                _pendingScanRanges.equals(that._pendingScanRanges);
    }

    @Override
    public int hashCode() {
        return _scanId.hashCode();
    }
}
