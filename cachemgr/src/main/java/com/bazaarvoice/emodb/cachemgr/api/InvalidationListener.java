package com.bazaarvoice.emodb.cachemgr.api;

public interface InvalidationListener {
    void handleInvalidation(InvalidationEvent event);
}
