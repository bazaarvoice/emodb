package com.bazaarvoice.emodb.sor.api;

import static com.google.common.base.Preconditions.checkNotNull;

public final class StashTimeKey {

    public static final String ZK_STRING_DELIMITER = "@";

    private final String _id;
    private final String _datacenter;

    public StashTimeKey(String id, String datacenter) {
        _id = checkNotNull(id, "id");
        _datacenter = checkNotNull(datacenter, "datacenter");
    }

    public static StashTimeKey of(String id, String datacenter) {
        return new StashTimeKey(id, datacenter);
    }

    public String getId() {
        return _id;
    }

    public String getDatacenter() {
        return _datacenter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StashTimeKey)) {
            return false;
        }
        StashTimeKey stashTimeInfoKey = (StashTimeKey) o;
        return _id.equals(stashTimeInfoKey._id) && _datacenter.equals(stashTimeInfoKey._datacenter);
    }

    @Override
    public int hashCode() {
        return 31 * _id.hashCode() + _datacenter.hashCode();
    }

    @Override
    public String toString() {
        return _id + "@" + _datacenter;
    }

    public static StashTimeKey fromString(String stashTimeKeyString) {
        String[] stashTimeKeyStringSplits = stashTimeKeyString.split(ZK_STRING_DELIMITER);
        if (stashTimeKeyStringSplits.length != 2) {
            throw new IllegalStateException("key string can only contain one '@' character. Pattern is ID@datacenter");
        }
        return StashTimeKey.of(stashTimeKeyStringSplits[0], stashTimeKeyStringSplits[1]);
    }
}