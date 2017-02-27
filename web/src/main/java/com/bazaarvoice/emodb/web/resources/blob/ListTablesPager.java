package com.bazaarvoice.emodb.web.resources.blob;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.Table;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import java.util.Iterator;

public class ListTablesPager extends AbstractIterator<Table> {
    private final BlobStore blobStore;
    private String lastTableSeen;
    private final Long pageSize;

    private Iterator<Table> page = Iterators.emptyIterator();

    ListTablesPager(final BlobStore blobStore, final String fromTableExclusive, final Long pageSize) {
        this.blobStore = blobStore;
        lastTableSeen = fromTableExclusive;
        this.pageSize = pageSize;
    }


    @Override protected Table computeNext() {
        if (!page.hasNext()) {
            page = blobStore.listTables(lastTableSeen, pageSize);
        }

        if (page.hasNext()) {
            final Table next = page.next();
            lastTableSeen = next.getName();
            return next;
        } else {
            endOfData();
            return null;
        }
    }
}
