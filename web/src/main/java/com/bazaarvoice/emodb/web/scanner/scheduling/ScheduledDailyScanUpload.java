package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

/**
 * POJO for holding details about a daily scan upload.
 */
public class ScheduledDailyScanUpload {

    private static final DateTimeFormatter TIME_OF_DAY_FORMAT = DateTimeFormat.forPattern("HH:mmZ").withZoneUTC();

    private final String _id;
    private final String _timeOfDay;
    private final DateTimeFormatter _scanIdFormat;
    private final ScanDestination _rootDestination;
    private final DateTimeFormatter _directoryFormat;
    private final List<String> _placements;
    private final int _maxRangeConcurrency;
    private final boolean _scanByAZ;
    private final boolean _requestRequired;
    private final int _maxRangeScanSplitSize;
    private final Duration _maxRangeScanTime;

    public ScheduledDailyScanUpload(String id, String timeOfDay, DateTimeFormatter scanIdFormat,
                                    ScanDestination rootDestination, DateTimeFormatter directoryFormat,
                                    List<String> placements, int maxRangeConcurrency,
                                    boolean scanByAZ, boolean requestRequired,
                                    int maxRangeScanSplitSize, Duration maxRangeScanTime) {
        _id = id;
        _timeOfDay = timeOfDay;
        _scanIdFormat = scanIdFormat;
        _rootDestination = rootDestination;
        _directoryFormat = directoryFormat;
        _placements = placements;
        _maxRangeConcurrency = maxRangeConcurrency;
        _scanByAZ = scanByAZ;
        _requestRequired = requestRequired;
        _maxRangeScanSplitSize = maxRangeScanSplitSize;
        _maxRangeScanTime = maxRangeScanTime;
    }

    public String getId() {
        return _id;
    }

    public String getTimeOfDay() {
        return _timeOfDay;
    }

    public DateTimeFormatter getScanIdFormat() {
        return _scanIdFormat;
    }

    public ScanDestination getRootDestination() {
        return _rootDestination;
    }

    public DateTimeFormatter getDirectoryFormat() {
        return _directoryFormat;
    }

    public List<String> getPlacements() {
        return _placements;
    }

    public int getMaxRangeConcurrency() {
        return _maxRangeConcurrency;
    }

    public boolean isScanByAZ() {
        return _scanByAZ;
    }

    public int getMaxRangeScanSplitSize() {
        return _maxRangeScanSplitSize;
    }

    public Duration getMaxRangeScanTime() {
        return _maxRangeScanTime;
    }

    public boolean isRequestRequired() {
        return _requestRequired;
    }

    /**
     * Gets the first execution time for the given scan and upload which is at or after "now".
     */
    public DateTime getNextExecutionTimeAfter(DateTime now) {
        DateTime timeOfDay = TIME_OF_DAY_FORMAT.parseDateTime(getTimeOfDay()).withZone(DateTimeZone.UTC);

        // The time of the next run is based on the time past midnight UTC relative to the current time
        DateTime nextExecTime = now.withZone(DateTimeZone.UTC)
                .withTimeAtStartOfDay()
                .plusMinutes(timeOfDay.getMinuteOfDay());

        // If the first execution would have been for earlier today move to the next execution.
        while (nextExecTime.isBefore(now)) {
            nextExecTime = nextExecTime.plusDays(1);
        }

        return nextExecTime;
    }
}