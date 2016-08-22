package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ForeignDataCenterEndPointProviderTest {

    @Test
    public void testEndPointProvider() {
        InvalidationServiceEndPointAdapter adapter = new InvalidationServiceEndPointAdapter("emodb-cachemgr",
                HostAndPort.fromString("localhost:8080"), HostAndPort.fromString("localhost:8081"));

        DataCenter dataCenter1 = mockDataCenter("us-east", "http://emodb.qa.us-east-1.nexus.bazaarvoice.com:8081");
        DataCenter dataCenter2 = mockDataCenter("us-west-2", "http://emodb.qa.us-west-2.nexus.bazaarvoice.com:8081");
        DataCenter dataCenter3 = mockDataCenter("eu-west-2", "http://emodb.qa.eu-west-2.nexus.bazaarvoice.com:8081");

        DataCenters dataCenters = mock(DataCenters.class);
        when(dataCenters.getSelf()).thenReturn(dataCenter3);
        when(dataCenters.getAll()).thenReturn(ImmutableList.of(dataCenter1, dataCenter2, dataCenter3));

        ForeignDataCenterEndPointProvider provider = new ForeignDataCenterEndPointProvider(adapter, dataCenters);

        final boolean[] called = {false};
        provider.withEndPoints(new Function<Collection<EndPoint>, Void>() {
            @Override
            public Void apply(Collection<EndPoint> endPoints) {
                called[0] = true;

                Set<String> expected = new HashSet<>();
                expected.add("http://emodb.qa.us-east-1.nexus.bazaarvoice.com:8081/tasks/invalidate");
                expected.add("http://emodb.qa.us-west-2.nexus.bazaarvoice.com:8081/tasks/invalidate");

                for (EndPoint endPoint : endPoints) {
                    Assert.assertTrue(endPoint.isValid());
                    Assert.assertTrue(expected.remove(endPoint.getAddress()));
                }
                Assert.assertTrue(expected.isEmpty());

                return null;
            }
        });
        Assert.assertTrue(called[0]);
    }

    private DataCenter mockDataCenter(String name, String adminUri) {
        DataCenter dc = mock(DataCenter.class);
        when(dc.getName()).thenReturn(name);
        when(dc.getAdminUri()).thenReturn(URI.create(adminUri));
        return dc;
    }
}
