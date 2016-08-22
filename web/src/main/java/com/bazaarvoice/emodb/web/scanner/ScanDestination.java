package com.bazaarvoice.emodb.web.scanner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * POJO for the destination for a scan and upload operation.  Can be either configured with a destination URI
 * or as a discarding destination.  Note that discarding is really only useful for unit testing, since a discarding
 * destination goes through the motions of a typical upload but then discards the results themselves.  (Think of
 * discarding as uploading to /dev/null.)
 */
public class ScanDestination {

    private final static String DISCARD = "discard";

    private final URI _uri;

    public static ScanDestination to(URI uri) {
        checkNotNull(uri, "uri");
        return new ScanDestination(uri);
    }

    public static ScanDestination discard() {
        return new ScanDestination(DISCARD);
    }

    @JsonCreator
    private ScanDestination(String dest) {
        URI uri = null;
        if (!DISCARD.equals(dest)) {
            uri = URI.create(dest);
        }
        _uri = uri;
    }

    private ScanDestination(@Nullable URI uri) {
        _uri = uri;
    }

    public boolean isDiscarding() {
        return _uri == null;
    }

    public URI getUri() {
        checkState(_uri != null, "Destination is null");
        return _uri;
    }

    /**
     * Creates a new scan destination at the given path rooted at the current scan destination.
     */
    public ScanDestination getDestinationWithSubpath(String path) {
        if (isDiscarding()) {
            return discard();
        }
        if (path == null) {
            return new ScanDestination(_uri);
        }
        return new ScanDestination(UriBuilder.fromUri(_uri).path(path).build());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScanDestination that = (ScanDestination) o;
        return Objects.equals(_uri, that._uri);
    }

    @Override
    public int hashCode() {
        return _uri != null ? _uri.hashCode() : 0;
    }

    @JsonValue
    public String toString() {
        return _uri != null ? _uri.toString() : DISCARD;
    }
}
