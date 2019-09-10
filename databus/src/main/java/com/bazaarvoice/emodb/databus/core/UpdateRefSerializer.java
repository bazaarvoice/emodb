package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.SetSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import org.apache.cassandra.db.marshal.UTF8Type;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class UpdateRefSerializer {
    private static final SetSerializer<String> _setSerializer = new SetSerializer<>(UTF8Type.instance);

    private static final List<Serializer<?>> _serializers = ImmutableList.<Serializer<?>>of(
            StringSerializer.get(),
            StringSerializer.get(),
            TimeUUIDSerializer.get(),
            BooleanSerializer.get(),
            LongSerializer.get(),
            DateSerializer.get()
            // TODO: CHECK IF WE NEED THIS MANUAL SERIALIZATION NECESSARY?? why not JSON default one??? FOR LATER!!!!!!
            );
    private static final List<String> _comparators = ImmutableList.of(
            StringSerializer.get().getComparatorType().getTypeName(),
            StringSerializer.get().getComparatorType().getTypeName(),
            TimeUUIDSerializer.get().getComparatorType().getTypeName(),
            BooleanSerializer.get().getComparatorType().getTypeName(),
            LongSerializer.get().getComparatorType().getTypeName(),
            DateSerializer.get().getComparatorType().getTypeName()
            );

    public static ByteBuffer toByteBuffer(UpdateRef ref) {
        Composite composite = newComposite();
        composite.add(ref.getTable());
        composite.add(ref.getKey());
        composite.add(ref.getChangeId());
        composite.add(ref.getIsDroppedUpdate());
        composite.add(ref.getVersion());
        composite.add(ref.getLastMutateAt());
        if (!ref.getTags().isEmpty()) {
            composite.add(_setSerializer.toByteBuffer(ref.getTags()).array());
        }
        return trim(composite.serialize());
    }

    public static UpdateRef fromByteBuffer(ByteBuffer buf) {
        Composite composite = newComposite();
        composite.deserialize(buf);

        String table = (String) composite.get(0);
        String key = (String) composite.get(1);
        UUID changeId = (UUID) composite.get(2);
        Boolean isDroppedUpdate = (Boolean) composite.get(3);
        Long version = (Long) composite.get(4);
        Date lastUpdateAt = (Date) composite.get(5);
        Set<String> tags = ImmutableSet.of();
        if (composite.size() == 7) {
            tags = _setSerializer.fromByteBuffer((ByteBuffer) composite.get(6));
        }
        return new UpdateRef(table, key, changeId, tags, isDroppedUpdate, version, lastUpdateAt);
    }

    private static Composite newComposite() {
        Composite composite = new Composite();
        composite.setSerializersByPosition(_serializers);
        composite.setComparatorsByPosition(_comparators);
        return composite;
    }

    /** Serialized Astyanax Composite objects use a lot of memory.  Trim it down. */
    private static ByteBuffer trim(ByteBuffer buf) {
        if (buf.capacity() <= 4 * buf.remaining()) {
            return buf;
        } else {
            ByteBuffer clone = ByteBuffer.allocate(buf.remaining());
            buf.get(clone.array());
            return clone;
        }
    }
}
