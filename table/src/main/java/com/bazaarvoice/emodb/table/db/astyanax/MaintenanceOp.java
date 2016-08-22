package com.bazaarvoice.emodb.table.db.astyanax;

import com.google.common.base.Objects;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Describes a pending table maintenance operation.
 */
class MaintenanceOp implements Comparable<MaintenanceOp> {
    private final String _name;
    private final DateTime _when;
    private final MaintenanceType _type;
    private final String _dataCenter;
    private MaintenanceTask _task;

    static MaintenanceOp forMetadata(String name, DateTime when, MaintenanceTask task) {
        return new MaintenanceOp(name, when, MaintenanceType.METADATA, "<system>", checkNotNull(task, "task"));
    }

    static MaintenanceOp forData(String name, DateTime when, String dataCenter, MaintenanceTask task) {
        return new MaintenanceOp(name, when, MaintenanceType.DATA, Objects.firstNonNull(dataCenter, "<n/a>"), checkNotNull(task, "task"));
    }

    static MaintenanceOp reschedule(MaintenanceOp op, DateTime when) {
        return new MaintenanceOp(op.getName(), when, op.getType(), op.getDataCenter(), op.getTask());
    }

    private MaintenanceOp(String name, DateTime when, MaintenanceType type, String dataCenter, MaintenanceTask task) {
        _name = checkNotNull(name, "name");
        _when = checkNotNull(when, "when");
        _type = checkNotNull(type, "type");
        _dataCenter = dataCenter;
        _task = task;
    }

    String getName() {
        return _name;
    }

    /** Returns the earliest time this maintenance should occur. */
    DateTime getWhen() {
        return _when;
    }

    /** Returns whether this maintenance is on the table metadata or on the table data.  It may not be both. */
    MaintenanceType getType() {
        return _type;
    }

    /** Returns the data center the maintenance should be performed in, if this maintenance is on table data. */
    String getDataCenter() {
        return _dataCenter;
    }

    MaintenanceTask getTask() {
        return _task;
    }

    void clearTask() {
        _task = null;
    }

    @Override
    public int compareTo(MaintenanceOp o) {
        return _when.compareTo(o.getWhen());
    }

    @Override
    public String toString() {
        return _name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MaintenanceOp)) {
            return false;
        }
        MaintenanceOp that = (MaintenanceOp) o;
        return _name.equals(that._name) &&
                _when.equals(that._when) &&
                _type == that._type &&
                Objects.equal(_dataCenter, that._dataCenter);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_name, _when, _type, _dataCenter);
    }
}
