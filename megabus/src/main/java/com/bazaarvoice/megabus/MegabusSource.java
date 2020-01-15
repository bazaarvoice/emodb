package com.bazaarvoice.megabus;

import java.util.Iterator;

/**
 * Defines the Interface for all ad-hoc Megabus Operations.
 */
public interface MegabusSource {

    void touch(String table, String key);

    void touchAll(Iterator<MegabusRef> refs);

    /** any other operations **/
}