package com.bazaarvoice.emodb.table.db.test;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.table.db.Table;

import java.util.Collection;
import java.util.Map;

public class InMemoryTable implements Table {
    private final String _name;
    private final TableOptions _options;
    private final boolean _facade;
    private Map<String, Object> _attributes;

    public InMemoryTable(String name, TableOptions options, Map<String, Object> attributes) {
        this(name, options, attributes, false);
    }

    public InMemoryTable(String name, TableOptions options, Map<String, Object> attributes, boolean facade) {
        _name = name;
        _options = options;
        _attributes = attributes;
        _facade = facade;
    }

    @Override
    public boolean isInternal() {
        return _name.startsWith("__");
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public TableOptions getOptions() {
        return _options;
    }

    @Override
    public TableAvailability getAvailability() {
        return new TableAvailability(_options.getPlacement(), _facade);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return _attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        _attributes = attributes;
    }

    @Override
    public Collection<DataCenter> getDataCenters() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFacade() {
        return _facade;
    }
}
