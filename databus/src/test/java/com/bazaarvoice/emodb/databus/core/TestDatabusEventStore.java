package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.event.api.EventStore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.mock;

class TestDatabusEventStore extends DatabusEventStore {
    TestDatabusEventStore() {
        super(mock(EventStore.class, doFail()), mock(DedupEventStore.class, doFail()), mock(ValueStore.class));
    }

    private static Answer doFail() {
        return new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                throw new AssertionError();
            }
        };
    }
}
