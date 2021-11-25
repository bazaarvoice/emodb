package com.bazaarvoice.emodb.common.zookeeper.store;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import io.dropwizard.lifecycle.Managed;

/**
 * Starts and stops a Guava service based on a ZooKeeper flag.
 */
public class GuavaServiceController implements Managed, ValueStoreListener {
    private final ValueStore<Boolean> _enabled;
    private final Supplier<? extends Service> _factory;
    private Service _service;
    private boolean _started;

    public GuavaServiceController(ValueStore<Boolean> enabled, Supplier<? extends Service> factory) {
        _enabled = enabled;
        _factory = factory;
    }

    @Override
    public synchronized void start() throws Exception {
        _started = true;
        _enabled.addListener(this);
        valueChanged();
    }

    @Override
    public synchronized void stop() throws Exception {
        _started = false;
        _enabled.removeListener(this);
        valueChanged();
    }

    @Override
    public synchronized void valueChanged() throws Exception {
        boolean enabled = _enabled.get() && _started;

        if (enabled && _service == null) {
            _service = _factory.get();
            _service.startAsync().awaitRunning();

        } else if (!enabled && _service != null) {
            try {
                _service.stopAsync().awaitTerminated();
            } finally {
                _service = null;
            }
        }
    }
}
