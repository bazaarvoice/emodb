package com.bazaarvoice.emodb.web.report;

/**
 * Thrown when querying for a report that does not exist.
 */
public class ReportNotFoundException extends RuntimeException {

    public ReportNotFoundException() {
    }

    public ReportNotFoundException(String s) {
        super(s);
    }
}
