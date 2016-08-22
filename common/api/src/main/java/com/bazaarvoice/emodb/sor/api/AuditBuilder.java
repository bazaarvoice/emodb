package com.bazaarvoice.emodb.sor.api;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

/**
 * Helper for building {@link Audit} objects.
 */
public final class AuditBuilder {

    // Delay computing the local hostname until we need it...
    private static final Supplier<String> _hostName = Suppliers.memoize(new Supplier<String>() {
        @Override
        public String get() {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (IOException e) {
                throw new AssertionError(e); // Should never happen
            }
        }
    });

    private final ImmutableMap.Builder<String, Object> _map = ImmutableMap.builder();

    public static AuditBuilder from(Audit audit) {
        return new AuditBuilder().setAll(audit.getAll());
    }

    public Audit build() {
        return new Audit(_map.build());
    }

    public AuditBuilder set(String key, Object value) {
        _map.put(key, value);
        return this;
    }

    public AuditBuilder setAll(Map<String, ?> map) {
        _map.putAll(map);
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
        return setHost(_hostName.get());
    }

    public AuditBuilder setUser(String user) {
        return set(Audit.USER, user);
    }
}
