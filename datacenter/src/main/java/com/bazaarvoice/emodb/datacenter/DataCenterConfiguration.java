package com.bazaarvoice.emodb.datacenter;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class DataCenterConfiguration {

    @Valid
    @NotNull
    private String _currentDataCenter;

    /**
     * The name of the current data center as configured in Cassandra's NetworkTopologyStrategy.  Defaults to the
     * current data center.
     */
    @Valid
    private String _cassandraDataCenter;

    @Valid
    @NotNull
    private String _systemDataCenter;

    @Valid
    @NotNull
    @JsonProperty("systemDataCenterServiceUri")
    private URI _systemDataCenterServiceUri;

    /**
     * Load-balanced highly available base URL for the EmoDB service (eg. http://localhost:8080).
     */
    @Valid
    @NotNull
    private URI _dataCenterServiceUri;

    /**
     * Load-balanced highly available base URL for the EmoDB administration tasks (eg. http://localhost:8081).
     */
    @Valid
    @NotNull
    private URI _dataCenterAdminUri;

    /**
     * Data centers which should be ignored.  This is uncommon and typically only applies when migrating or removing an
     * an existing data center.  This allows for a smooth transition from the old to the new data center without any
     * downtime or fanout errors if either data center unreachable.
     **/
    @Valid
    @NotNull
    private Set<String> _ignoredDataCenters = Collections.EMPTY_SET;

    public boolean isSystemDataCenter() {
        return _currentDataCenter.equals(_systemDataCenter);
    }

    public String getCurrentDataCenter() {
        return _currentDataCenter;
    }

    public DataCenterConfiguration setCurrentDataCenter(String currentDataCenter) {
        _currentDataCenter = currentDataCenter;
        return this;
    }

    public String getCassandraDataCenter() {
        return Optional.ofNullable(_cassandraDataCenter).orElse(_currentDataCenter);
    }

    public DataCenterConfiguration setCassandraDataCenter(String cassandraDataCenter) {
        _cassandraDataCenter = cassandraDataCenter;
        return this;
    }

    public String getSystemDataCenter() {
        return _systemDataCenter;
    }

    public DataCenterConfiguration setSystemDataCenter(String systemDataCenter) {
        _systemDataCenter = systemDataCenter;
        return this;
    }

    public URI getDataCenterServiceUri() {
        return _dataCenterServiceUri;
    }

    public DataCenterConfiguration setDataCenterServiceUri(URI dataCenterServiceUri) {
        _dataCenterServiceUri = dataCenterServiceUri;
        return this;
    }

    public URI getDataCenterAdminUri() {
        return _dataCenterAdminUri;
    }

    public DataCenterConfiguration setDataCenterAdminUri(URI dataCenterAdminUri) {
        _dataCenterAdminUri = dataCenterAdminUri;
        return this;
    }

    public URI getSystemDataCenterServiceUri() {
        return _systemDataCenterServiceUri;
    }

    public DataCenterConfiguration setIgnoredDataCenters(Set<String> ignoredDataCenters) {
        _ignoredDataCenters = ignoredDataCenters;
        return this;
    }

    public Set<String> getIgnoredDataCenters() {
        return _ignoredDataCenters;
    }
}
