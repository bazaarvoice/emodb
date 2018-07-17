package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

/**
 * POJO class for representing unpublished databus events.
 */
@JsonIgnoreProperties (ignoreUnknown = true)
public class UnpublishedDatabusEvent {

    private final String _table;
    private final Date _date;
    private final UnpublishedDatabusEventType _eventType;

    @JsonCreator
    public UnpublishedDatabusEvent(@JsonProperty ("table") String table, @JsonProperty ("date") Date date, @JsonProperty ("event") UnpublishedDatabusEventType eventType) {
        _table = table;
        _date = date;
        _eventType = eventType;
    }

    public String getTable() {
        return _table;
    }

    public Date getDate() {
        return _date;
    }

    public UnpublishedDatabusEventType getEventType() {
        return _eventType;
    }

    @Override
    public String toString() {
        return JsonHelper.asJson(this);
    }
}
