package com.bazaarvoice.emodb.table.db.test;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEvent;
import com.bazaarvoice.emodb.sor.api.FacadeExistsException;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEventType;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownFacadeException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.MoveType;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class InMemoryTableDAO implements TableDAO {

    private final Map<String, Long> _nameToUuid = Maps.newTreeMap();
    private final Map<Long, Table> _uuidToTable = Maps.newHashMap();

    @Override
    public Iterator<Table> list(@Nullable String fromNameExclusive, final LimitCounter limit) {
        return _uuidToTable.values().iterator();
    }

    @Override
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(DateTime fromInclusive, DateTime toExclusive) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeUnpublishedDatabusEvent(String name, UnpublishedDatabusEventType attribute){
        // no-op.
    }

    @Override
    public void create(String name, TableOptions options, Map<String, ?> attributes, Audit audit) {
        // Check that the table doesn't yet exist.
        Long existingUuid = _nameToUuid.get(name);
        if (existingUuid != null) {
            Table existingTable = _uuidToTable.get(existingUuid);
            if (existingTable.getOptions().getPlacement().equals(options.getPlacement())
                    && existingTable.getAttributes().equals(attributes)) {
                // Allow re-creating a table with no changes as a way of forcing a global cache flush.
                return;
            } else {
                throw new TableExistsException(format("Cannot create table that already exists: %s", name), name);
            }
        }

        long newUuid = Hashing.murmur3_128().hashUnencodedChars(name).asLong();
        while (_uuidToTable.containsKey(newUuid) || newUuid == -1) {
            newUuid += 1;
        }
        _nameToUuid.put(name, newUuid);
        _uuidToTable.put(newUuid, new InMemoryTable(name, options, Maps.newHashMap(attributes)));
    }

    @Override
    public void createFacade(String name, FacadeOptions options, Audit audit)
            throws FacadeExistsException {
        throw new UnsupportedOperationException("Facades are currently unsupported in InMemory table");
    }

    @Override
    public boolean checkFacadeAllowed(String name, FacadeOptions options)
            throws TableExistsException {
        throw new UnsupportedOperationException("Facades are currently unsupported in InMemory table");
    }

    @Override
    public void drop(String name, Audit audit) {
        if (_nameToUuid.remove(name) == null) {
            throw new UnknownTableException(format("Unknown table: %s", name), name);
        }
        // todo: doesn't remove any data
    }

    @Override
    public void dropFacade(String name, String placement, Audit audit)
            throws UnknownFacadeException {
        throw new UnsupportedOperationException("Facades are currently unsupported in InMemory table");
    }

    @Override
    public void move(String name, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType) throws UnknownTableException {
        // no-op
    }

    @Override
    public void moveFacade(String name, String sourcePlacement, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType) throws UnknownTableException {
        throw new UnsupportedOperationException("Facades are currently unsupported in InMemory table");
    }

    @Override
    public void setAttributes(String name, Map<String, ?> attributes, Audit audit) throws UnknownTableException {
        ((InMemoryTable) get(name)).setAttributes(Maps.newHashMap(attributes));
    }

    @Override
    public void audit(String name, String op, Audit audit) {
        // no-op
    }

    @Override
    public boolean exists(String name) {
        return _nameToUuid.containsKey(name);
    }

    @Override
    public boolean isMoveToThisPlacementAllowed(String placement) {
        return true;
    }

    @Override
    public Table get(String name) {
        Long tableUuid = _nameToUuid.get(name);
        if (tableUuid == null) {
            throw new UnknownTableException(format("Unknown table: %s", name), name);
        }
        return _uuidToTable.get(tableUuid);
    }

    @Override
    public Table getByUuid(long uuid) throws UnknownTableException, DroppedTableException {
        Table table = _uuidToTable.get(uuid);
        if (table == null) {
            throw new UnknownTableException(format("Unknown table: %s", uuid));
        }
        return table;
    }

    @Override
    public Collection<String> getTablePlacements(boolean includeInternal, boolean localOnly) {
        List<String> placements = Lists.newArrayList();
        placements.addAll(ImmutableList.of("app_global:default", "ugc_global:ugc", "catalog_global:cat"));
        if (includeInternal) {
            placements.add("app_global:sys");
        }
        return placements;
    }

    @Override
    public TableSet createTableSet() {
        return new TableSet() {
            private final LoadingCache<Long, Optional<Table>> snapshot = CacheBuilder.newBuilder()
                    .build(new CacheLoader<Long, Optional<Table>>() {
                        @Override
                        public Optional<Table> load(Long uuid)
                                throws Exception {
                            Table table = _uuidToTable.get(uuid);
                            if (table == null) {
                                return Optional.absent();
                            }
                            return Optional.<Table>of(new InMemoryTable(table.getName(), table.getOptions(),
                                    ImmutableMap.copyOf(table.getAttributes()), table.isFacade()));
                        }
                    });

            @Override
            public Table getByUuid(long uuid) {
                Optional<Table> table = snapshot.getUnchecked(uuid);
                if (!table.isPresent()) {
                    throw new UnknownTableException(format("Unknown table: %s", uuid));
                }
                return table.get();
            }

            @Override
            public void close()
                    throws IOException {
                // no-op
            }
        };
    }
}
