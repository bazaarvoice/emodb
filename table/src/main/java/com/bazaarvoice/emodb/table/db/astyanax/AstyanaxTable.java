package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.table.db.Table;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Contrary to the name, AstyanaxTable is not actually specific to Astyanax, and its use does not imply any dependency
 * on Astyanax itself.
 * TODO: Rename class to "BackendTable" as it has nothing to do with Astyanax
 */
public class AstyanaxTable implements Table {
    private final String _name;
    private final TableOptions _options;
    private final Map<String, Object> _attributes;
    private final TableAvailability _availability;
    private final AstyanaxStorage _readStorage;
    private final Collection<AstyanaxStorage> _writeStorage;
    private final Supplier<Collection<DataCenter>> _dataCenters;

    public AstyanaxTable(String name, TableOptions options,
                         Map<String, Object> attributes, @Nullable TableAvailability availability,
                         AstyanaxStorage readStorage, Collection<AstyanaxStorage> writeStorage,
                         Supplier<Collection<DataCenter>> dataCenters) {
        _name = checkNotNull(name, "name");
        _options = checkNotNull(options, "options");
        _attributes = checkNotNull(attributes, "attributes");
        _availability = availability;
        _readStorage = checkNotNull(readStorage, "readStorage");
        _writeStorage = checkNotNull(writeStorage, "writeStorage");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
    }

    @Override
    public boolean isInternal() {
        return _name.startsWith("__");
    }

    @Override
    public boolean isFacade() {
        return _availability != null && _availability.isFacade();
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public TableOptions getOptions() {
        return _options;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return _attributes;
    }

    @Override
    public TableAvailability getAvailability() {
        return _availability;
    }

    @Override
    public Collection<DataCenter> getDataCenters() {
        // Calculate the data centers on demand since they may change in a live system.
        return _dataCenters.get();
    }

    public AstyanaxStorage getReadStorage() {
        return _readStorage;
    }

    public Collection<AstyanaxStorage> getWriteStorage() {
        return _writeStorage;
    }

    /**
     * Test if a given UUID matches this table.
     */
    boolean hasUUID(long uuid) {
        if (_readStorage.hasUUID(uuid)) {
            return true;
        }
        for (AstyanaxStorage storage : _writeStorage) {
            if (storage.hasUUID(uuid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a placeholder for unknown tables.  A table may be unknown for one of two reasons:  1) the table
     * used to exist but has been deleted, or 2) the table never existed.  In the former case the table will have
     * a name, but in the latter case the table will be unnamed and the "name" parameter will be null.
     */
    public static AstyanaxTable createUnknown(long uuid, Placement placement, @Nullable String name) {
        AstyanaxStorage storage = new AstyanaxStorage(
                uuid, RowKeyUtils.NUM_SHARDS_UNKNOWN, false/*reads-not-allowed*/, placement.getName(), Suppliers.ofInstance(placement));
        return new AstyanaxTable(
                name != null ? name : "__unknown:" + TableUuidFormat.encode(uuid),
                new TableOptionsBuilder().setPlacement(placement.getName()).build(),
                ImmutableMap.<String, Object>of("~unknown", true, "uuid", uuid),
                null/*availability*/,
                storage, ImmutableList.of(storage),
                Suppliers.<Collection<DataCenter>>ofInstance(ImmutableList.<DataCenter>of()));
    }

    public boolean isUnknownTable() {
        Object unknownAttribute = getAttributes().get("~unknown");
        return unknownAttribute != null && Boolean.TRUE.equals(unknownAttribute);
    }

    // for debugging
    @Override
    public String toString() {
        return _name;
    }
}
