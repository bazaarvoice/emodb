package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;

import java.util.Set;

/**
 * Simple {@link StashRequestDAO} implementation which uses an in-memory map.  Useful for unit testing.
 */
public class InMemoryStashRequestDAO implements StashRequestDAO {

    private Table<String, String, StashRequest> _requestStore = HashBasedTable.create();

    @Override
    public void requestStash(String scanId, StashRequest request) {
        _requestStore.put(scanId, request.getRequestedBy(), request);
    }

    @Override
    public void undoRequestStash(String scanId, StashRequest request) {
        _requestStore.remove(scanId, request.getRequestedBy());
    }

    @Override
    public Set<StashRequest> getRequestsForStash(String scanId) {
        return ImmutableSet.copyOf(_requestStore.row(scanId).values());
    }
}
