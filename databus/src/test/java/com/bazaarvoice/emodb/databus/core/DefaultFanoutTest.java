package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.table.db.Table;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Date;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class DefaultFanoutTest {

    private DefaultFanout _defaultFanout;
    private Supplier<Iterable<OwnedSubscription>> _subscriptionsSupplier;
    private DataCenter _currentDataCenter;
    private DataCenter _remoteDataCenter;
    private DataProvider _dataProvider;
    private DatabusAuthorizer _databusAuthorizer;
    private String _remoteChannel;
    private Multimap<String, ByteBuffer> _eventsSinked;
    private PartitionSelector _outboundPartitionSelector;

    @BeforeMethod
    private void setUp() {
        _eventsSinked = ArrayListMultimap.create();

        Function<Multimap<String, ByteBuffer>, Void> eventSink = new Function<Multimap<String, ByteBuffer>, Void>() {
            @Override
            public Void apply(Multimap<String, ByteBuffer> input) {
                synchronized (this) {
                    _eventsSinked.putAll(input);
                }
                return null;
            }
        };

        _subscriptionsSupplier = mock(Supplier.class);
        _currentDataCenter = mock(DataCenter.class);
        when(_currentDataCenter.getName()).thenReturn("local");
        _remoteDataCenter = mock(DataCenter.class);
        when(_remoteDataCenter.getName()).thenReturn("remote");
        _remoteChannel = ChannelNames.getReplicationFanoutChannel(_remoteDataCenter, 0);

        RateLimitedLogFactory rateLimitedLogFactory = mock(RateLimitedLogFactory.class);
        when(rateLimitedLogFactory.from(any(Logger.class))).thenReturn(mock(RateLimitedLog.class));

        _dataProvider = mock(DataProvider.class);
        _databusAuthorizer = mock(DatabusAuthorizer.class);

        SubscriptionEvaluator subscriptionEvaluator = new SubscriptionEvaluator(
                _dataProvider, _databusAuthorizer, rateLimitedLogFactory);

        _outboundPartitionSelector = mock(PartitionSelector.class);
        when(_outboundPartitionSelector.getPartition(anyString())).thenReturn(0);

        MetricRegistry metricRegistry = new MetricRegistry();

        _defaultFanout = new DefaultFanout("test", "test", mock(EventSource.class), eventSink, _outboundPartitionSelector,
                Duration.standardSeconds(1), _subscriptionsSupplier, _currentDataCenter, rateLimitedLogFactory, subscriptionEvaluator,
                new FanoutLagMonitor(mock(LifeCycleRegistry.class), metricRegistry), metricRegistry, Clock.systemUTC());
    }

    @Test
    public void testMatchingTable() {
        addTable("matching-table");

        OwnedSubscription subscription = new DefaultOwnedSubscription(
                "test", Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("matching-table")),
                new Date(), Duration.standardDays(1), "owner0");

        EventData event = newEvent("id0", "matching-table", "key0");

        when(_subscriptionsSupplier.get()).thenReturn(ImmutableList.of(subscription));
        DatabusAuthorizer.DatabusAuthorizerByOwner authorizerByOwner = mock(DatabusAuthorizer.DatabusAuthorizerByOwner.class);
        when(authorizerByOwner.canReceiveEventsFromTable("matching-table")).thenReturn(true);
        when(_databusAuthorizer.owner("owner0")).thenReturn(authorizerByOwner);

        _defaultFanout.copyEvents(ImmutableList.of(event));

        assertEquals(_eventsSinked,
                ImmutableMultimap.of("test", event.getData(), _remoteChannel, event.getData()));
    }

    @Test
    public void testNotMatchingTable() {
        addTable("other-table");

        OwnedSubscription subscription = new DefaultOwnedSubscription(
                "test", Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("not-matching-table")),
                new Date(), Duration.standardDays(1), "owner0");

        EventData event = newEvent("id0", "other-table", "key0");

        when(_subscriptionsSupplier.get()).thenReturn(ImmutableList.of(subscription));
        DatabusAuthorizer.DatabusAuthorizerByOwner authorizerByOwner = mock(DatabusAuthorizer.DatabusAuthorizerByOwner.class);
        when(authorizerByOwner.canReceiveEventsFromTable("matching-table")).thenReturn(true);
        when(_databusAuthorizer.owner("owner0")).thenReturn(authorizerByOwner);

        _defaultFanout.copyEvents(ImmutableList.of(event));

        // Event does not match subscription, should only go to remote fanout
        assertEquals(_eventsSinked,
                ImmutableMultimap.of(_remoteChannel, event.getData()));
    }

    @Test
    public void testUnauthorizedFanout() {
        addTable("unauthorized-table");

        OwnedSubscription subscription = new DefaultOwnedSubscription(
                "test", Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("unauthorized-table")),
                new Date(), Duration.standardDays(1), "owner0");

        EventData event = newEvent("id0", "unauthorized-table", "key0");

        when(_subscriptionsSupplier.get()).thenReturn(ImmutableList.of(subscription));
        DatabusAuthorizer.DatabusAuthorizerByOwner authorizerByOwner = mock(DatabusAuthorizer.DatabusAuthorizerByOwner.class);
        when(authorizerByOwner.canReceiveEventsFromTable("matching-table")).thenReturn(false);
        when(_databusAuthorizer.owner("owner0")).thenReturn(authorizerByOwner);

        _defaultFanout.copyEvents(ImmutableList.of(event));

        // Event is not authorized for owner, should only go to remote fanout
        assertEquals(_eventsSinked,
                ImmutableMultimap.of(_remoteChannel, event.getData()));

    }

    @Test
    public void testFanoutToMultiplePartitions() {
        reset(_outboundPartitionSelector);

        when(_outboundPartitionSelector.getPartition("key0")).thenReturn(0);
        when(_outboundPartitionSelector.getPartition("key1")).thenReturn(1);
        when(_outboundPartitionSelector.getPartition("key2")).thenReturn(2);
        when(_outboundPartitionSelector.getPartition("key3")).thenReturn(0);

        addTable("partition-test-table");

        List<String> remoteChannels = Lists.newArrayListWithCapacity(3);
        for (int partition=0; partition < 3; partition++) {
            remoteChannels.add(ChannelNames.getReplicationFanoutChannel(_remoteDataCenter, partition));
        }
        
        List<EventData> events = Lists.newArrayListWithCapacity(4);
        for (int i=0; i < 4; i++) {
            EventData event = newEvent("id" + i, "partition-test-table", "key" + i);
            events.add(event);
        }

        when(_subscriptionsSupplier.get()).thenReturn(ImmutableList.of());
        _defaultFanout.copyEvents(ImmutableList.copyOf(events));

        assertEquals(ImmutableSetMultimap.copyOf(_eventsSinked),
                ImmutableSetMultimap.builder()
                        .put(remoteChannels.get(0), events.get(0).getData())
                        .put(remoteChannels.get(1), events.get(1).getData())
                        .put(remoteChannels.get(2), events.get(2).getData())
                        .put(remoteChannels.get(0), events.get(3).getData())
                        .build());
    }

    private void addTable(String tableName) {
        Table table = mock(Table.class);
        when(table.getName()).thenReturn(tableName);
        when(table.getAttributes()).thenReturn(ImmutableMap.<String, Object>of());
        when(table.getOptions()).thenReturn(new TableOptionsBuilder().setPlacement("placement").build());
        // Put in another data center to force replication
        when(table.getDataCenters()).thenReturn(ImmutableList.of(_currentDataCenter, _remoteDataCenter));
        when(_dataProvider.getTable(tableName)).thenReturn(table);
    }

    private EventData newEvent(String id, String table, String key) {
        EventData eventData = mock(EventData.class);
        when(eventData.getId()).thenReturn(id);

        UpdateRef updateRef = new UpdateRef(table, key, TimeUUIDs.newUUID(), ImmutableSet.<String>of());
        ByteBuffer data = UpdateRefSerializer.toByteBuffer(updateRef);
        when(eventData.getData()).thenReturn(data);

        return eventData;
    }
}
