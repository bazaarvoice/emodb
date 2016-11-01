package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.bazaarvoice.emodb.common.zookeeper.store.ZkTimestampSerializer;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkValueSerializer;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Simple serializer for storing {@link StashRunTimeInfo} configurations in ZooKeeper.
 */
public class StashRunTimeInfoSerializer implements ZkValueSerializer<StashRunTimeInfo> {

    private static final ZkTimestampSerializer TIMESTAMP_SERIALIZER = new ZkTimestampSerializer();

    @Override
    public String toString(StashRunTimeInfo stashRunTimeInfo) {
        return String.format("%s;%s;%s;%s", TIMESTAMP_SERIALIZER.toString(stashRunTimeInfo.getTimestamp()), stashRunTimeInfo.getDataCenter(),
                stashRunTimeInfo.getExpiredTimestamp(), StringUtils.join(stashRunTimeInfo.getPlacements(), ','));
    }

    @Override
    public StashRunTimeInfo fromString(String string) {
        if (string == null) {
            return null;
        }
        try {
            List<String> strings = Arrays.asList(StringUtils.split(string, ";"));
            Long timestamp = TIMESTAMP_SERIALIZER.fromString(strings.get(0));
            String dataCenter = strings.get(1);
            Long expiredTimestamp = TIMESTAMP_SERIALIZER.fromString(strings.get(2));
            List<String> placements = Arrays.asList(StringUtils.split(strings.get(3), ","));

            return new StashRunTimeInfo(timestamp, placements, dataCenter, expiredTimestamp);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("StashRunTimeInfo string must be of the form \"timestamp;datacenter;remote;placement1,placement2,placement3,...\"");
        }
    }
}

