package com.bazaarvoice.emodb.web.throttling;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

/**
 * POJO to describe a request method and path that has an ad-hoc throttle set.
 */
public class AdHocThrottleEndpoint {

    private final static String SLASH_REPLACEMENT_STRING = "~";

    private final String _method;
    private final String _path;

    public AdHocThrottleEndpoint(String method, String path) {
        _method = requireNonNull(method, "method").toUpperCase();
        requireNonNull(path, "path");
        // Remove the leading slash if it was explicitly provided
        if (path.startsWith("/")) {
            _path = path.substring(1);
        } else {
            _path = path;
        }
    }

    public String getMethod() {
        return _method;
    }

    public String getPath() {
        return _path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AdHocThrottleEndpoint)) {
            return false;
        }

        AdHocThrottleEndpoint that = (AdHocThrottleEndpoint) o;

        return _method.equals(that._method) && _path.equals(that._path);
    }

    @Override
    public int hashCode() {
        return hash(_method, _path);
    }

    /**
     * The string form of AdHocThrottleEndpoint is used as a key in a {@link com.bazaarvoice.emodb.common.zookeeper.store.MapStore}.
     * For this reason the string version cannot contain the '/' character.
     */
    @Override
    public String toString() {
        return _method + "_" + _path.replaceAll("/", SLASH_REPLACEMENT_STRING);
    }

    public static AdHocThrottleEndpoint fromString(String string) {
        int firstUnderbar = string.indexOf('_');
        checkArgument(firstUnderbar > 0, "Method separator not found");
        String method = string.substring(0, firstUnderbar);
        String path = string.substring(firstUnderbar + 1).replaceAll(SLASH_REPLACEMENT_STRING, "/");
        return new AdHocThrottleEndpoint(method, path);
    }
}
