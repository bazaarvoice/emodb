package com.bazaarvoice.emodb.web.report;

import com.google.common.base.Objects;

import java.util.Date;

public class AllTablesReportResult {

    private String _reportId;
    private Date _startTime;
    private Date _completeTime;
    private boolean _success;
    private String _lastCompletePlacement;
    private int _lastCompleteShard;
    private String _lastCompleteTableUuid;

    public String getReportId() {
        return _reportId;
    }

    public void setReportId(String reportId) {
        _reportId = reportId;
    }

    public AllTablesReportResult withReportId(String reportId) {
        setReportId(reportId);
        return this;
    }

    public Date getStartTime() {
        return _startTime;
    }

    public void setStartTime(Date startTime) {
        _startTime = startTime;
    }

    public AllTablesReportResult withStartTime(Date startTime) {
        setStartTime(startTime);
        return this;
    }

    public Date getCompleteTime() {
        return _completeTime;
    }

    public void setCompleteTime(Date completeTime) {
        _completeTime = completeTime;
    }

    public AllTablesReportResult withCompleteTime(Date completeTime) {
        setCompleteTime(completeTime);
        return this;
    }

    public boolean isSuccess() {
        return _success;
    }

    public void setSuccess(boolean success) {
        _success = success;
    }

    public AllTablesReportResult withSuccess(boolean success) {
        setSuccess(success);
        return this;
    }

    public String getLastCompletePlacement() {
        return _lastCompletePlacement;
    }

    public void setLastCompletePlacement(String lastCompletePlacement) {
        _lastCompletePlacement = lastCompletePlacement;
    }

    public AllTablesReportResult withLastCompletePlacement(String lastCompletePlacement) {
        setLastCompletePlacement(lastCompletePlacement);
        return this;
    }

    public int getLastCompleteShard() {
        return _lastCompleteShard;
    }

    public void setLastCompleteShard(int lastCompleteShard) {
        _lastCompleteShard = lastCompleteShard;
    }

    public AllTablesReportResult withLastCompleteShard(int lastCompleteShard) {
        setLastCompleteShard(lastCompleteShard);
        return this;
    }

    public String getLastCompleteTableUuid() {
        return _lastCompleteTableUuid;
    }

    public void setLastCompleteTableUuid(String lastCompleteTableUuid) {
        _lastCompleteTableUuid = lastCompleteTableUuid;
    }

    public AllTablesReportResult withLastCompleteTableUuid(String lastCompleteTableUuid) {
        setLastCompleteTableUuid(lastCompleteTableUuid);
        return this;
    }

    public String toString() {
        return Objects.toStringHelper(this)
                .omitNullValues()
                .add("reportId", getReportId())
                .add("success", isSuccess())
                .add("startTime", getStartTime())
                .add("completeTime", getCompleteTime())
                .add("lastCompletePlacement", getLastCompletePlacement())
                .add("lastCompleteShard", getLastCompleteShard() > 0 ? getLastCompleteShard() : null)
                .add("lastComplateTableUuid", getLastCompleteTableUuid())
                .toString();
    }
}
