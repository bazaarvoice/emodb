package com.bazaarvoice.emodb.sortedq.api;

import com.bazaarvoice.emodb.sortedq.db.QueueDAO;

public interface SortedQueueFactory {
    SortedQueue create(String name, QueueDAO dao);
    SortedQueue create(String name, boolean readOnly, QueueDAO dao);
}
