package com.bazaarvoice.megabus;

import com.bazaarvoice.megabus.resource.Coordinate;

import java.util.Iterator;

/**
 * Defines the Interface for all ad-hoc Megabus Operations.
 */
public interface MegabusSource {

    void touch(String table, String key);

    void touchAll(Iterator<Coordinate> coordinates);

    /** any other operations **/
}
