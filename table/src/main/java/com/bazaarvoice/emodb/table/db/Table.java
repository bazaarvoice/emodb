package com.bazaarvoice.emodb.table.db;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;

import java.util.Collection;
import java.util.Map;

public interface Table {

    boolean isInternal();

    boolean isFacade();

    String getName();

    TableOptions getOptions();

    TableAvailability getAvailability();

    Map<String, Object> getAttributes();

    Collection<DataCenter> getDataCenters();
}
