package com.bazaarvoice.emodb.table.db.astyanax;

/** An exception that indicates that there are pending table events that are preventing table maintenance from occurring */
class PendingTableEventsException extends RuntimeException {

    PendingTableEventsException(String storageUuid) {
        super(storageUuid);
    }
}
