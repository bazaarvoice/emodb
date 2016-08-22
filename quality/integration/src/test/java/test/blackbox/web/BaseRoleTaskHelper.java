package test.blackbox.web;

import com.google.common.collect.ImmutableMap;

/**
 * Helper defines task entry point smoke test for tasks registered in EmoDB
 */
public abstract class BaseRoleTaskHelper extends BaseRoleConnectHelper {


    BaseRoleTaskHelper(String configFile) {
        super (configFile);
    }

    protected void testLeaderServiceTaskAccessible () throws Exception {
        // POST    /tasks/leader (com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask)
        httpPost(ImmutableMap.<String, Object>of("release", "test"), "tasks", "leader");
    }

    protected void testThrottleControlTaskAccessible () throws Exception {
        // POST    /tasks/blacklist (com.bazaarvoice.emodb.web.throttling.ThrottleControlTask)
        httpPost(ImmutableMap.<String, Object>of("add", "122.0.0.1,122.0.0.2"), "tasks", "blacklist");
    }

    protected void testDropwizardInvalidationTaskAccessible () throws Exception {
        // POST    /tasks/invalidate (com.bazaarvoice.emodb.cachemgr.invalidate.DropwizardInvalidationTask)
        httpPost(ImmutableMap.<String, Object>of("cache", "test", "scope", "LOCAL"), "tasks", "invalidate");
    }

    protected void testHintsConsistencyTimeTaskAccessible () throws Exception {
        // POST    /tasks/sor-compaction-timestamp (com.bazaarvoice.emodb.sor.consistency.HintsConsistencyTimeTask)
        // curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-timestamp?all=PT30M'
        httpPost(ImmutableMap.<String, Object>of("all", "PT30M"), "tasks", "sor-compaction-timestamp");
    }

    protected void testMinLagDurationTaskAccessible () throws Exception {
        // POST    /tasks/sor-compaction-lag (com.bazaarvoice.emodb.sor.consistency.MinLagDurationTask)
        // curl -s -XPOST 'http://localhost:8081/tasks/sor-compaction-lag?emo_cluster=PT12H'
        httpPost(ImmutableMap.<String, Object>of("emo_cluster", "PT12H"), "tasks", "sor-compaction-lag");
    }

    protected void testSorMoveTableTaskAccessible () throws Exception {
        // POST    /tasks/sor-move (com.bazaarvoice.emodb.table.db.astyanax.MoveTableTask)
        // curl -s -XPOST http://localhost:8081/tasks/sor-move
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "sor-move");
    }

    protected void testSorTableChangesEnabledTaskAccessible () throws Exception {
        // POST    /tasks/sor-table-changes (com.bazaarvoice.emodb.table.db.astyanax.TableChangesEnabledTask)
        // curl -s -XPOST http://localhost:8081/tasks/sor-table-changes
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "sor-table-changes");
    }

    protected void testRowKeyTaskAccessible () throws Exception {
        // POST    /tasks/sor-row-key (com.bazaarvoice.emodb.sor.admin.RowKeyTask)
        // $ curl -s -XPOST http://localhost:8081/tasks/sor-row-key?coord=review:testcustomer/demo1
        // review:testcustomer/demo1: 564c0c4f54555e41e664656d6f31
        httpPost(ImmutableMap.<String, Object>of("coord", "review:testcustomer/demo1"), "tasks", "sor-row-key");
    }

    protected void testBlobMoveTableTaskAccessible () throws Exception {
        // POST    /tasks/blob-move (com.bazaarvoice.emodb.table.db.astyanax.MoveTableTask)
        // curl -s -XPOST http://localhost:8081/tasks/blob-move
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "blob-move");
    }

    protected void testBlobTableChangesEnabledTaskAccessible () throws Exception {
        // POST    /tasks/blob-table-changes (com.bazaarvoice.emodb.table.db.astyanax.TableChangesEnabledTask)
        // curl -s -XPOST http://localhost:8081/tasks/blob-table-changes
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "blob-table-changes");
    }

    protected void testMaintenanceRateLimitTaskAccessible () throws Exception {
        // POST    /tasks/sor-rate-limits (com.bazaarvoice.emodb.table.db.astyanax.MaintenanceRateLimitTask)
        // POST    /tasks/blob-rate-limits (com.bazaarvoice.emodb.table.db.astyanax.MaintenanceRateLimitTask)
        // curl -s -XPOST "http://localhost:8081/tasks/sor-rate-limits"
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "sor-rate-limits");
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "blob-rate-limits");
    }

    protected void testReplicationEnabledTaskAccessible () throws Exception {
        // POST    /tasks/busrepl (com.bazaarvoice.emodb.databus.repl.ReplicationEnabledTask)
        // curl -s -XPOST http://localhost:8081/tasks/busrepl
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "busrepl");
    }

    protected void testDedupMigrationTaskAccessible () throws Exception {
        // POST    /tasks/dedup-databus-migration (com.bazaarvoice.emodb.databus.core.DedupMigrationTask)
        // curl -s -XPOST http://localhost:8081/tasks/dedup-databus-migration?dedup=true
        httpPost(ImmutableMap.<String, Object>of("dedup", "true"), "tasks", "dedup-databus-migration");
    }

    protected void testClaimCountTaskAccessible() throws Exception {
        // POST    /tasks/claims-databus (com.bazaarvoice.emodb.event.admin.ClaimCountTask)
        // POST    /tasks/claims-queue (com.bazaarvoice.emodb.event.admin.ClaimCountTask)
        // curl -s -XPOST http://localhost:8081/tasks/claims-databus
        // curl -s -XPOST http://localhost:8081/tasks/claims-queue
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "claims-databus");
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "claims-queue");
    }

    protected void testDedupQueueTaskAccessible() throws Exception {
        // POST    /tasks/dedup-databus (com.bazaarvoice.emodb.event.admin.DedupQueueTask)
        // POST    /tasks/dedup-queue (com.bazaarvoice.emodb.event.admin.DedupQueueTask)
        // curl -s -XPOST http://localhost:8081/tasks/dedup-databus
        // curl -s -XPOST http://localhost:8081/tasks/dedup-queue
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "dedup-databus");
        httpPost(ImmutableMap.<String, Object>of(), "tasks", "dedup-queue");
    }

    protected void testScanApiAccessible () throws Exception {
        // POST    /tasks/leader (com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask)
        httpPost(ImmutableMap.<String, Object>of("release", "test"), "tasks", "leader");
    }
}
