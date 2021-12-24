package com.bazaarvoice.emodb.event.api;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Specifies instances of {@link DedupEventStore} should store data internally.
 */
public abstract class DedupEventStoreChannels {
    /**
     * The event store channel names used by the dedup queue don't publicly overlap with those used
     * by the regular event store (although if you know the prefix convention you can back door in).
     * In practice, this separates QueueService regular queues from dedup queues.
     */
    public static DedupEventStoreChannels isolated(String writePrefix, String readPrefix) {
        return new Impl(writePrefix, readPrefix);
    }

    /**
     * The event store channel name used to buffer writes doesn't have a special prefix.  Apps can use
     * the regular event store to write, and those events can be consumed by the dedup event store.
     * In practice, this is used to provide a migration path from pre-dedup Databus queues to dedup
     * databus queues--the old event store channels become the new dedup queue write channels and
     * existing data doesn't require a special migration step.
     */
    public static DedupEventStoreChannels sharedWriteChannel(String readPrefix) {
        return new Impl("", readPrefix);
    }

    /** Returns the name of the event store channel that events are written to before being dedup'd. */
    public abstract String writeChannel(String queue);

    /** Returns the name of the event store channel that events are read from after being dedup'd. */
    public abstract String readChannel(String queue);

    /** Returns the name of the dedup queue, given the name of the queue's write channel. */
    @Nullable
    public abstract String queueFromWriteChannel(String channel);

    /** Returns the name of the dedup queue, given the name of the queue's read channel. */
    @Nullable
    public abstract String queueFromReadChannel(String channel);

    private static class Impl extends DedupEventStoreChannels {
        private final String _writePrefix;
        private final String _readPrefix;

        private Impl(String writePrefix, String readPrefix) {
            _writePrefix = requireNonNull(writePrefix, "writePrefix");
            _readPrefix = requireNonNull(readPrefix, "readPrefix");
            checkArgument(!_readPrefix.equals(_writePrefix));
        }

        @Override
        public String writeChannel(String queue) {
            return _writePrefix + queue;
        }

        @Override
        public String readChannel(String queue) {
            return _readPrefix + queue;
        }

        @Nullable
        @Override
        public String queueFromWriteChannel(String channel) {
            return queueFromChannel(channel, _writePrefix, _readPrefix);
        }

        @Nullable
        @Override
        public String queueFromReadChannel(String channel) {
            return queueFromChannel(channel, _readPrefix, _writePrefix);
        }

        @Nullable
        private static String queueFromChannel(String channel, String requiredPrefix, String disallowedPrefix) {
            // If the channel startsWith both required and disallowed, assume it matches the longer prefix.
            if (channel.startsWith(disallowedPrefix) && disallowedPrefix.length() > requiredPrefix.length()) {
                return null;
            }
            return removePrefix(channel, requiredPrefix);
        }

        @Nullable
        private static String removePrefix(String string, String prefix) {
            return string.startsWith(prefix) ? string.substring(prefix.length()) : null;
        }
    }
}
