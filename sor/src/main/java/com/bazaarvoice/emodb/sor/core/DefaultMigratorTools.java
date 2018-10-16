package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.bazaarvoice.emodb.sor.db.MigratorReaderDAO;
import com.bazaarvoice.emodb.sor.db.MigratorWriterDAO;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.table.db.StashTableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.bazaarvoice.emodb.table.db.consistency.HintsConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.stash.StashTokenRange;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultMigratorTools implements MigratorTools {

    private final MigratorReaderDAO _migratorReaderDao;
    private final MigratorWriterDAO _migratorWriterDao;
    private final FullConsistencyTimeProvider _fullConsistencyTimeProvider;
    private final PlacementCache _placementCache;
    private final StashTableDAO _stashTableDao;

    @Inject
    public DefaultMigratorTools(MigratorReaderDAO migratorReaderDAO, MigratorWriterDAO migratorWriterDAO,
                                HintsConsistencyTimeProvider fullConsistencyTimeProvider, PlacementCache placementCache,
                                StashTableDAO stashTableDAO) {
        _migratorReaderDao = checkNotNull(migratorReaderDAO, "migratorReaderDao");
        _migratorWriterDao = checkNotNull(migratorWriterDAO, "migratorWriterDao");
        _fullConsistencyTimeProvider = checkNotNull(fullConsistencyTimeProvider, "fullConsistencyTimeProvider");
        _placementCache = checkNotNull(placementCache, "placementCache");
        _stashTableDao = checkNotNull(stashTableDAO, "stashTableDao");
    }

    @Override
    public void writeRows(String placement, Iterator<MigrationScanResult> results, RateLimiter rateLimiter) {
        checkNotNull(placement, "placement");
        checkNotNull(results, "rows");
        _migratorWriterDao.writeRows(placement, results, rateLimiter);
    }

    @Override
    public Iterator<MigrationScanResult> readRows(String placement, ScanRange scanRange) {
        checkNotNull(placement, "placement");
        checkNotNull(scanRange, "scanRange");

        // Since the range may wrap from high to low end of the token range we need to unwrap it
        List<ScanRange> unwrappedRanges = scanRange.unwrapped();

        Iterator<StashTokenRange> tokenRanges = Iterators.concat(
                Iterators.transform(
                        unwrappedRanges.iterator(),
                        unwrappedRange -> _stashTableDao.getStashTokenRangesFromSnapshot(placement, placement, unwrappedRange.getFrom(), unwrappedRange.getTo())));

        return Iterators.concat(
                Iterators.transform(tokenRanges, tokenRange ->
                        _migratorReaderDao.readRows(placement, ScanRange.create(tokenRange.getFrom(), tokenRange.getTo()))
                )
        );
    }

    @Override
    public long getFullConsistencyTimestamp(String placement) {
        return _fullConsistencyTimeProvider.getMaxTimeStamp(_placementCache.get(placement).getKeyspace().getClusterName());
    }
}
