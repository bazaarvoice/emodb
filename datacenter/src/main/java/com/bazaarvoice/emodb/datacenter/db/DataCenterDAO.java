package com.bazaarvoice.emodb.datacenter.db;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;

import javax.annotation.Nullable;
import java.util.Map;

public interface DataCenterDAO {

    Map<String, DataCenter> loadAll();

    boolean saveIfChanged(DataCenter dataCenter, @Nullable DataCenter original);
}
