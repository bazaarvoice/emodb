package com.bazaarvoice.emodb.event.core;

import com.google.common.base.Objects;

/**
 * Creates a size cache key, such that it compares only on the channel name, but also contains limitAsked
 * so that the loading function has that info embedded in the key, but at the same time we only have one
 * key per channel in the cache.
 */
public class SizeCacheKey {
    public String channelName;
    public long limitAsked;

    public SizeCacheKey(String channelName, long limitAsked) {
        this.channelName = channelName;
        this.limitAsked = limitAsked;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(channelName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SizeCacheKey)) {
            return false;
        }
        SizeCacheKey that = (SizeCacheKey) o;
        return Objects.equal(channelName, that.channelName);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("channelName", channelName)
                .add("limitAsked", limitAsked)
                .toString();
    }
}
