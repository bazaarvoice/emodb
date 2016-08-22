package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferSerializer;

import java.nio.ByteBuffer;

import static java.lang.String.format;

public abstract class AbstractPlacementFactory implements PlacementFactory {

    protected <C> ColumnFamily<ByteBuffer, C> getColumnFamily(KeyspaceDefinition keyspaceDef,
                                                              String prefix, String suffix, String placement,
                                                              Serializer<C> columnSerializer) throws IllegalArgumentException {
        // Create the column family object.  It must be keyed by a ByteBuffer because that's what
        // the AstyanaxTable.getRowKey() method returns.
        ColumnFamily<ByteBuffer, C> cf = new ColumnFamily<>(prefix + "_" + suffix,
                ByteBufferSerializer.get(), columnSerializer);

        // Verify that the column family exists in the Cassandra schema.
        ColumnFamilyDefinition cfDef = keyspaceDef.getColumnFamily(cf.getName());
        if (cfDef == null) {
            throw new UnknownPlacementException(format(
                    "Placement string '%s' refers to unknown Cassandra %s column family in keyspace '%s': %s",
                    placement, suffix, keyspaceDef.getName(), cf.getName()), placement);
        }
        return cf;
    }
}
