package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.zookeeper.Sync;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkValueSerializer;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dropwizard task to update the full consistency control values for each cluster.
 * See subclasses for usage instructions.
 */
abstract class ConsistencyControlTask<T> extends Task {
    private static final Logger _log = LoggerFactory.getLogger(ConsistencyControlTask.class);

    public static final String ALL_CLUSTERS = "all";

    private final String _description;
    private final Map<String, ValueStore<T>> _valueMap;
    private final CuratorFramework _curator;
    private final ZkValueSerializer<T> _serializer;
    private final Supplier<T> _defaultSupplier;

    public ConsistencyControlTask(TaskRegistry taskRegistry,
                                  String name, String description,
                                  Map<String, ValueStore<T>> valueMap,
                                  CuratorFramework curator,
                                  ZkValueSerializer<T> serializer,
                                  Supplier<T> defaultSupplier) {
        super(name);
        _description = checkNotNull(description, "description");
        _valueMap = checkNotNull(valueMap, "valueMap");
        _curator = checkNotNull(curator, "curator");
        _serializer = checkNotNull(serializer, "serializer");
        _defaultSupplier = defaultSupplier;
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) throws Exception {
        boolean changed = false;
        for (Map.Entry<String, String> entry : parameters.entries()) {
            String cluster = entry.getKey();
            String durationOrTimestamp = entry.getValue();

            // A blank string will reset the value back to its default.
            T value = null;
            if (!Strings.isNullOrEmpty(durationOrTimestamp)) {
                try {
                    value = fromString(durationOrTimestamp);
                } catch (Exception e) {
                    out.printf("Unable to parse value '%s: %s%n", durationOrTimestamp, e);
                    continue;
                }
            }

            if (ALL_CLUSTERS.equals(cluster)) {
                for (ValueStore<T> holder : _valueMap.values()) {
                    update(holder, value, cluster);
                    changed = true;
                }
            } else {
                ValueStore<T> holder = _valueMap.get(cluster);
                if (holder == null) {
                    out.printf("Unknown cluster: %s%n", cluster);
                    continue;
                }
                update(holder, value, cluster);
                changed = true;
            }
        }

        if (changed) {
            // Wait briefly for round trip through ZooKeeper.
            if (!Sync.synchronousSync(_curator, Duration.ofSeconds(1))) {
                out.println("WARNING: Timed out after one second waiting for updates to round trip through ZooKeeper.");
            }
            Thread.sleep(50);  // Wait a little bit longer for NodeCache listeners to fire.
        }

        out.printf("%s:%n", _description);
        for (Map.Entry<String, ValueStore<T>> entry : _valueMap.entrySet()) {
            String cluster = entry.getKey();
            T value = entry.getValue().get();
            String string;
            if (value != null) {
                string = toString(value);
            } else {
                string = toString(_defaultSupplier.get()) + " [DEFAULT]";
            }
            out.printf("%s: %s%n", cluster, string);
        }
    }

    private void update(ValueStore<T> holder, T value, String cluster) throws Exception {
        holder.set(value);
        _log.info("{} for cluster '{}' set to: {}", _description, cluster, value);
    }

    protected T fromString(String string) {
        return _serializer.fromString(string);
    }

    protected String toString(T value) {
        return _serializer.toString(value);
    }
}
