package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class DatabusChannelConfigurationTest {
    @Test
    public void testGetTTLForReplay() {
        OwnedSubscription replaySubscription = new DefaultOwnedSubscription(ChannelNames.getMasterReplayChannel(),
                Conditions.alwaysTrue(), Date.from(Instant.now().plus(Duration.ofDays(3650))),
                DatabusChannelConfiguration.REPLAY_TTL, "id");
        SubscriptionDAO mockSubscriptionDao = mock(SubscriptionDAO.class);
        when(mockSubscriptionDao.getSubscription(ChannelNames.getMasterReplayChannel())).thenReturn(replaySubscription);
        DatabusChannelConfiguration dbusConf =
                new DatabusChannelConfiguration(mockSubscriptionDao);
        Duration replayTtl = dbusConf.getEventTtl(ChannelNames.getMasterReplayChannel());
        assertEquals(replayTtl, DatabusChannelConfiguration.REPLAY_TTL, "Replay_TTL is incorrect");
    }
}
