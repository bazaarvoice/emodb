package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;

import java.util.Set;

/**
 * Simple {@link ScanRequestDAO} implementation which uses an in-memory map.  Useful for unit testing.
 */
public class InMemoryScanRequestDAO implements ScanRequestDAO {

    private Table<String, String, ScanRequest> _requestStore = HashBasedTable.create();

    @Override
    public void requestScan(String scanId, ScanRequest request) {
        _requestStore.put(scanId, request.getRequestedBy(), request);
    }

    @Override
    public void undoRequestScan(String scanId, ScanRequest request) {
        _requestStore.remove(scanId, request.getRequestedBy());
    }

    @Override
    public Set<ScanRequest> getRequestsForScan(String scanId) {
        return ImmutableSet.copyOf(_requestStore.row(scanId).values());
    }
}
