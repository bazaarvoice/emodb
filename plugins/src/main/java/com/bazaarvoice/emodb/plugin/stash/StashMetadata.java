package com.bazaarvoice.emodb.plugin.stash;

import java.net.URI;
import java.util.Date;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Metadata about a Stash run passed to {@link StashStateListener}.
 */
public class StashMetadata {

    private final String _id;
    private final Date _startTime;
    private final Set<String> _placements;
    private final Set<URI> _destinations;

    public StashMetadata(String id, Date startTime, Set<String> placements, Set<URI> destinations) {
        _id = requireNonNull(id, "id");
        _startTime = requireNonNull(startTime, "startTime");
        _placements = requireNonNull(placements, "placements");
        _destinations = requireNonNull(destinations, "destinations");
    }

    public String getId() {
        return _id;
    }

    public Date getStartTime() {
        return _startTime;
    }

    public Set<String> getPlacements() {
        return _placements;
    }

    public Set<URI> getDestinations() {
        return _destinations;
    }
}
