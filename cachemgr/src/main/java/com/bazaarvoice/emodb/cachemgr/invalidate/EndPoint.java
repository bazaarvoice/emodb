package com.bazaarvoice.emodb.cachemgr.invalidate;

public interface EndPoint {

    /**
     * Return the implementation-specific address (usually a url) for this end point.
     */
    String getAddress();

    /**
     * Is this end point still expected to be valid?
     */
    boolean isValid();
}
