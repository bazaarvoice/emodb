package com.bazaarvoice.emodb.sor.api;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper for building {@link Audit} objects.
 */
public final class AuditBuilder {

    // Delay computing the local hostname until we need it...
    private static final AtomicReference<String> _hostName = new AtomicReference<>(null);

    private final Map<String, Object> _map = new LinkedHashMap<>();

    public static AuditBuilder from(Audit audit) {
        return new AuditBuilder().setAll(audit.getAll());
    }

    public Audit build() {
        return new Audit(Collections.unmodifiableMap(_map));
    }

    public AuditBuilder set(String key, Object value) {
        if (_map.put(key, value) != null) {
            throw new IllegalStateException("Key has already been set");
        }
        return this;
    }

    public AuditBuilder setAll(Map<String, ?> map) {
        map.entrySet().forEach(e -> set(e.getKey(), e.getValue()));
        return this;
    }

    public AuditBuilder setComment(String comment) {
        return set(Audit.COMMENT, comment);
    }

    public AuditBuilder setProgram(String program) {
        return set(Audit.PROGRAM, program);
    }

    public AuditBuilder setHost(String host) {
        return set(Audit.HOST, host);
    }

    public AuditBuilder setLocalHost() {
        return setHost(_hostName.updateAndGet(host -> {
            if (host != null) {
                return host;
            }
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (IOException e) {
                throw new AssertionError(e); // Should never happen
            }
        }));
    }

    public AuditBuilder setUser(String user) {
        return set(Audit.USER, user);
    }
}
