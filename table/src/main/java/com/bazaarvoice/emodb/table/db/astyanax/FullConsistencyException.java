package com.bazaarvoice.emodb.table.db.astyanax;

/** An exception that indicates that full consistency as not been reached as expected/required. */
class FullConsistencyException extends RuntimeException {
    FullConsistencyException(String message) {
        super(message);
    }
}
