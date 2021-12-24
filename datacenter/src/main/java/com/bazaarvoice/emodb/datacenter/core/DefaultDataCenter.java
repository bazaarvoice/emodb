package com.bazaarvoice.emodb.datacenter.core;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;

import java.net.URI;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

public class DefaultDataCenter implements DataCenter {
    private final String _name;
    private final URI _serviceUri;
    private final URI _adminUri;
    private final boolean _system;
    private final String _cassandraName;
    private final Collection<String> _cassandraKeyspaces;

    public DefaultDataCenter(String name, URI serviceUri, URI adminUri, boolean system,
                             String cassandraName, Collection<String> cassandraKeyspaces) {
        _name = requireNonNull(name, "name");
        _serviceUri = requireNonNull(serviceUri, "serviceUri");
        _adminUri = requireNonNull(adminUri, "adminUri");
        _system = system;
        _cassandraName = requireNonNull(cassandraName, "cassandraName");
        _cassandraKeyspaces = requireNonNull(cassandraKeyspaces, "cassandraKeyspaces");
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public URI getServiceUri() {
        return _serviceUri;
    }

    @Override
    public URI getAdminUri() {
        return _adminUri;
    }

    @Override
    public boolean isSystem() {
        return _system;
    }

    @Override
    public String getCassandraName() {
        return _cassandraName;
    }

    @Override
    public Collection<String> getCassandraKeyspaces() {
        return _cassandraKeyspaces;
    }

    /**
     * Equality is based on the data center name only.  Two {@code DataCenter} objects are equal if they refer to
     * the same data center.
     */
    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof DataCenter && _name.equals(((DataCenter) o).getName()));
    }

    /**
     * The hash code is based on the data center name only.  Two {@code DataCenter} objects are equal if they refer
     * to the same data center.
     */
    @Override
    public int hashCode() {
        return _name.hashCode();
    }

    @Override
    public int compareTo(DataCenter o) {
        return _name.compareTo(o.getName());
    }

    // For debugging
    @Override
    public String toString() {
        return _name;
    }
}
