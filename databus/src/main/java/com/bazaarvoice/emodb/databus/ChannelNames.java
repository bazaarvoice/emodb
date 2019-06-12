package com.bazaarvoice.emodb.databus;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.event.api.DedupEventStoreChannels;

public class ChannelNames {
    private static final String SYSTEM_PREFIX = "__system_bus:";
    private static final String MASTER_FANOUT = SYSTEM_PREFIX + "master";
    private static final String REPLICATION_FANOUT_PREFIX = SYSTEM_PREFIX + "out:";
    private static final String MASTER_REPLAY = SYSTEM_PREFIX + "replay";
    private static final String MASTER_CANARY_PREFIX = SYSTEM_PREFIX + "canary";
    private static final String MEGABUS_REF_PRODUCER_PREFIX = SYSTEM_PREFIX + "megabus";

    private static final DedupEventStoreChannels DEDUP_CHANNELS =
            DedupEventStoreChannels.sharedWriteChannel("__dedupq_read:");

    public static boolean isSystemChannel(String channel) {
        return channel.startsWith(SYSTEM_PREFIX);
    }

    public static boolean isSystemFanoutChannel(String channel) {
        return channel.startsWith(MASTER_FANOUT) || channel.startsWith(REPLICATION_FANOUT_PREFIX);
    }

    public static String getMegabusRefProducerChannel(String applicationId, int partition) {
        return String.format("%s-%s-%d", MEGABUS_REF_PRODUCER_PREFIX, applicationId, partition);
    }

    public static String getMasterFanoutChannel(int partition) {
        return String.format("%s[%d]", MASTER_FANOUT, partition);
    }

    public static String getReplicationFanoutChannel(DataCenter dataCenter, int partition) {
        return String.format("%s%s[%d]", REPLICATION_FANOUT_PREFIX, dataCenter.getName(), partition);
    }

    public static String getMasterReplayChannel() {
        return MASTER_REPLAY;
    }

    public static String getMasterCanarySubscription(String cluster) {
        return MASTER_CANARY_PREFIX + "-" + cluster.toLowerCase().replace(" ", "-");
    }

    public static boolean isNonDeduped(String channel) {
        // The canary is the only internal system channel that gets deduped.  If, for some oddball reason,
        // someone tries to use a subscription name that overlaps with our internal "__dedupq_read:" or
        // "_dedupq_write:", don't dedup it.  Just provide raw access to the underlying event channel.
        return isSystemChannel(channel) && !channel.startsWith(MASTER_CANARY_PREFIX) ||
                channel.startsWith("__dedupq_");
    }

    public static DedupEventStoreChannels dedupChannels() {
        return DEDUP_CHANNELS;
    }
}
