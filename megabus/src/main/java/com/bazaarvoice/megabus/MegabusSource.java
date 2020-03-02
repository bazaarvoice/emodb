package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.sor.api.Coordinate;

import java.util.Iterator;

/**
 * Defines the Interface for all ad-hoc Megabus Operations.
 */
public interface MegabusSource {

    void touch(Coordinate coordinate);

    void touchAll(Iterator<Coordinate> coordinates);

    /** any other operations **/
}
