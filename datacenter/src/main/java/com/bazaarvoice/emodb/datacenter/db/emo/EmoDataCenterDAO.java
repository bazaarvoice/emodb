package com.bazaarvoice.emodb.datacenter.db.emo;

import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.core.DefaultDataCenter;
import com.bazaarvoice.emodb.datacenter.db.DataCenterDAO;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


/**
 * Stores all data center information in a single SoR object.  Use a single object so that reading the set of all
 * data centers is inexpensive.
 */
public class EmoDataCenterDAO implements DataCenterDAO {
    private static final String TABLE = "__system_sor:data_center";
    private static final String ROW = "data_center";

    private final DataStore _dataStore;
    private final String _cluster;

    @Inject
    public EmoDataCenterDAO(DataStore dataStore, @ServerCluster String cluster) {
        _dataStore = Objects.requireNonNull(dataStore, "dataStore");
        _cluster = Objects.requireNonNull(cluster, "cluster");
    }

    @Override
    public Map<String, DataCenter> loadAll() {
        Map<String, Object> json = _dataStore.get(TABLE, ROW);

        return deserializeAll(json);
    }

    @Override
    public boolean saveIfChanged(DataCenter dataCenter, @Nullable DataCenter original) {
        Map.Entry<String, Object> entry = serialize(dataCenter);

        // Compare against the original using the serialized form.
        if (original != null && entry.equals(serialize(original))) {
            return false;
        }

        Delta delta = Deltas.mapBuilder()
                .put(entry.getKey(), entry.getValue())
                .build();

        Audit audit = new AuditBuilder().setLocalHost().setProgram("emodb").build();
        _dataStore.update(TABLE, ROW, TimeUUIDs.newUUID(), delta, audit);
        return true;
    }

    private Map<String, DataCenter> deserializeAll(Map<String, Object> json) {
        Map<String, DataCenter> m = new HashMap<>();
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            if (!entry.getKey().startsWith("~")) {
                DataCenter dataCenter = deserialize(entry);
                if (dataCenter != null) {
                    m.put(dataCenter.getName(), dataCenter);
                }
            }
        }
        return Collections.unmodifiableMap(m);
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private DataCenter deserialize(Map.Entry<String, Object> entry) {
        String name = entry.getKey();
        Map<String, Object> map = (Map<String, Object>) entry.getValue();
        String cluster = (String) map.get("cluster");
        // If running against a data set restored from backup from another cluster, ignore left-over data center entries.
        if (cluster != null && !_cluster.equals(cluster)) {
            return null;
        }
        URI serviceUri = URI.create((String) map.get("serviceUri"));
        URI adminUri = URI.create((String) map.get("adminUri"));
        boolean system = (Boolean) map.get("system");
        String cassandraName = Optional.ofNullable((String) map.get("cassandraName")).orElse(name);
        List<String> cassandraKeyspaces = Optional.ofNullable((List<String>) map.get("cassandraKeyspaces")).orElse(Collections.emptyList());
        return new DefaultDataCenter(name, serviceUri, adminUri, system, cassandraName, cassandraKeyspaces);
    }

    private Map.Entry<String, Object> serialize(DataCenter dataCenter) {
        Map<String, Object> m = new HashMap<>();
        m.put("cluster", _cluster);
        m.put("serviceUri", dataCenter.getServiceUri().toString());
        m.put("adminUri", dataCenter.getAdminUri().toString());
        m.put("system", dataCenter.isSystem());
        m.put("cassandraName", dataCenter.getCassandraName());
        m.put("cassandraKeyspaces", sorted(dataCenter.getCassandraKeyspaces()));
        Map<String, Object> result = new HashMap<>();
        result.put(dataCenter.getName(), m);
        return result.entrySet().stream().findFirst().get();
    }

    private static List<String> sorted(Collection<String> col) {
        List<String> list = new ArrayList<>(col);
        Collections.sort(list);
        return list;
    }
}
