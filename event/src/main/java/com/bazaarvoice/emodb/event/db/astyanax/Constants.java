package com.bazaarvoice.emodb.event.db.astyanax;

import java.time.Duration;

interface Constants {

    /** Maximum number of events that should be stored in a single slab.  Must be <= 65536. */
    static final int MAX_SLAB_SIZE = 1000;

    /** Maximum number of bytes that should be in a slab */
    static final int BYTES_PER_MEGABYTE = 1024 * 1024;
    static final int MAX_SLAB_SIZE_IN_MEGABYTES = 10;
    static final int MAX_SLAB_SIZE_IN_BYTES = MAX_SLAB_SIZE_IN_MEGABYTES * BYTES_PER_MEGABYTE;
    static final int MAX_EVENT_SIZE_IN_BYTES = 256 * 1024;

    /** Slab column value that indicates a slab is still open. */
    static final int OPEN_SLAB_MARKER = Integer.MAX_VALUE;

    /** A server is expected to update a slab at least this often or else close it. */
    static final Duration OPEN_SLAB_MARKER_TTL = Duration.ofMinutes(20);

    /** Don't keep slabs open for too long.  Rotate them periodically.
     * This used to be one day, instead of one hour, but it was lowered to make databus replay more efficient.
     */
    static final Duration SLAB_ROTATE_TTL = Duration.ofHours(1);

    /** Add some extra padding to databus replay time to avoid any unknown race conditions */
    static final Duration REPLAY_PADDING_TIME = Duration.ofMinutes(5);

    /** Cap the size of updates. */
    static final int MUTATION_MAX_ROWS = 10;
    static final int MUTATION_MAX_COLUMNS = 1000;
}
