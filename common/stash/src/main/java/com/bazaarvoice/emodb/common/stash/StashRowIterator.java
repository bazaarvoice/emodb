package com.bazaarvoice.emodb.common.stash;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;

/**
 * Base interface for iterating over Stash content.  The primary purpose is to provide a closeable iterator since
 * the iterator may be holding connections to S3.
 */
public interface StashRowIterator extends Iterator<Map<String, Object>>, Closeable {
}
