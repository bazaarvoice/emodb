package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.tableset.BlockFileTableSet;
import com.bazaarvoice.emodb.table.db.tableset.DistributedTableSerializer;
import com.bazaarvoice.emodb.web.scanner.ScannerZooKeeper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides an abstraction for creating and cleanup up distributed TableSets used by scan uploads.
 */
public class ScanTableSetManager {

    private final static String TABLE_SETS_PATH = "/table-sets";

    private final CuratorFramework _curator;
    private final DataTools _dataTools;

    @Inject
    public ScanTableSetManager(@ScannerZooKeeper CuratorFramework curator, DataTools dataTools) {
        _curator = checkNotNull(curator, "curator");
        _dataTools = checkNotNull(dataTools, "dataTools");
    }

    /**
     * Return the IDs of all scans that have distributed table sets.
     */
    public List<String> getAvailableTableSets() {
        try {
            return _curator.getChildren().forPath(TABLE_SETS_PATH);
        } catch (KeeperException.NoNodeException e) {
            // Ok, no available table sets
            return ImmutableList.of();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Returns a distributed table set for the provided scan ID.
     */
    public TableSet getTableSetForScan(String id) {
        TableSet source = _dataTools.createTableSet();
        String path = getPathForScan(id);

        DistributedTableSerializer tableSerializer = new DistributedTableSerializer(source, _curator, path);
        return new BlockFileTableSet(tableSerializer);
    }

    /**
     * Deletes all artifacts created by the TableSet during the run for the provided scan ID.  This method should
     * be called at some point after the scan is complete.
     */
    public void cleanupTableSetForScan(String id) {
        String path = getPathForScan(id);
        DistributedTableSerializer.cleanup(_curator, path);
    }

    private String getPathForScan(String id) {
        return ZKPaths.makePath(TABLE_SETS_PATH, id);
    }
}
