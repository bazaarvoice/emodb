package com.bazaarvoice.emodb.event.db.astyanax;

import java.time.Duration;

interface Constants {

    /** Maximum number of events that should be stored in a single slab.  Must be <= 65536. */
    static final int MAX_SLAB_SIZE = 1000;

    /** Slab column value that indicates a slab is still open. */
    static final int OPEN_SLAB_MARKER = Integer.MAX_VALUE;

    /** A server is expected to update a slab at least this often or else close it. */
    static final Duration OPEN_SLAB_MARKER_TTL = Duration.ofMinutes(20);

    /** Don't keep slabs open for too long.  Rotate them periodically. */
    static final Duration SLAB_ROTATE_TTL = Duration.ofDays(1);

    /** Cap the size of updates. */
    static final int MUTATION_MAX_ROWS = 10;
    static final int MUTATION_MAX_COLUMNS = 1000;
}
