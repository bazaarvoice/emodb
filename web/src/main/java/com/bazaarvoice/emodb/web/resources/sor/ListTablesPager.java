package com.bazaarvoice.emodb.web.resources.sor;

import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Table;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.util.Iterator;

public class ListTablesPager extends AbstractIterator<Table> {
    private final DataStore dataStore;
    private String lastTableSeen;
    private final Long pageSize;

    private Iterator<Table> page = Iterators.emptyIterator();

    ListTablesPager(final DataStore dataStore, final String fromTableExclusive, final Long pageSize) {
        this.dataStore = dataStore;
        lastTableSeen = fromTableExclusive;
        this.pageSize = pageSize;
    }


    @Override protected Table computeNext() {
        if (!page.hasNext()) {
            page = dataStore.listTables(lastTableSeen, pageSize);
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
