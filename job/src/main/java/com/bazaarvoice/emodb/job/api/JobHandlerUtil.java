package com.bazaarvoice.emodb.job.api;

abstract public class JobHandlerUtil {

    private JobHandlerUtil() {
        // empty
    }

    public static boolean isNotOwner(JobHandler<?, ?> handler) {
        return handler.isNotOwner();
    }
}
