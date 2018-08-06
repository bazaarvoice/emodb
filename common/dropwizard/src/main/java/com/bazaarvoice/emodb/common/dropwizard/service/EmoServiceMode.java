package com.bazaarvoice.emodb.common.dropwizard.service;

import java.util.Collections;
import java.util.EnumSet;

/**
 * Enumeration for selecting the mode in which Emo DropWizard service is running.
 *
 * Options are:
 *
 * <ul>
 * <li>STANDARD_ALL:
 *      All services, tasks, and resources required for a standard EmoDB server are started</li>
 * <li>STANDARD_MAIN:
 *      All services NOT broken out into separate roles</li>
 * <li>STANDARD_BLOB:
 *      Only services, tasks, and resources required to support BlobStore role.</li>
 * <li>QUEUE_ROLE:
 *      Only services, tasks, and resources required to support Queue role.</li>
 * <li>CLI_TOOL:
 *      Only those services, tasks, and resources required to run non-server applications are started.
 *      A typical use case for this is running command-line tools, such as reports.</li>
 * <li>SCANNER:
 *      Only services, tasks, and resources required to support the scan and upload server.</li>
 * <li>SCANNER:
 *      Only services, tasks, and resources required to migrate all delta from old tables to new "blocked" tables.</li>
 * </ul>
 */
public enum EmoServiceMode {

    STANDARD_ALL(Aspect.getStandardAspects()),

    STANDARD_MAIN(
            Aspect.web,
            Aspect.task,
            Aspect.cache,
            Aspect.dataCenter,
            Aspect.dataCenter_announce,
            Aspect.leader_control,
            Aspect.background_table_maintenance,
            Aspect.sor_zookeeper_full_consistency,
            Aspect.dataStore_web,
            Aspect.dataStore_module,
            Aspect.blobStore_module, // needed for table maintenance
            Aspect.dataBus_web,
            Aspect.dataBus_module,
            Aspect.dataBus_fan_out_and_replication,
            Aspect.queue_web,
            Aspect.queue_module,
            Aspect.blackList,
            Aspect.throttle,
            Aspect.report,
            Aspect.compaction_control,
            Aspect.compaction_control_web,
            Aspect.job,
            Aspect.security,
            Aspect.full_consistency,
            Aspect.invalidation_cache_listener,
            Aspect.swagger,
            Aspect.uac
    ),

    STANDARD_BLOB(
            Aspect.web,
            Aspect.task,
            Aspect.cache,
            Aspect.dataCenter,
            Aspect.dataCenter_announce,
            Aspect.dataStore_module, // needed for creating tables, etc.
            Aspect.blobStore_web,
            Aspect.blobStore_module,
            Aspect.blackList,
            Aspect.throttle,
            Aspect.compaction_control,
            Aspect.security,
            Aspect.leader_control, // needed for HintsPollerManager
            Aspect.blob_zookeeper_full_consistency,
            Aspect.full_consistency,
            Aspect.invalidation_cache_listener,
            Aspect.swagger
    ),

    CLI_TOOL(
            Aspect.task,
            Aspect.cache,
            Aspect.dataStore_module,
            Aspect.blobStore_module,
            Aspect.dataBus_module,
            Aspect.queue_module,
            Aspect.security
    ),

    SCANNER(
            Aspect.web,
            Aspect.cache,
            Aspect.leader_control,
            Aspect.dataCenter,
            Aspect.dataCenter_announce,
            Aspect.dataStore_module,
            Aspect.blobStore_module, // needed for permission resolver
            Aspect.scanner,
            Aspect.compaction_control,
            Aspect.security,
            Aspect.full_consistency
    ),

    DELTA_MIGRATOR(
            Aspect.web,
            Aspect.cache,
            Aspect.leader_control,
            Aspect.dataCenter,
            Aspect.dataStore_module,
            Aspect.blobStore_module, // needed for permission resolver
            Aspect.delta_migrator,
            Aspect.compaction_control,
            Aspect.security,
            Aspect.full_consistency
    );

    private final EnumSet<Aspect> aspects;

    private EmoServiceMode (EnumSet<Aspect> selected) {
        aspects = selected;
    }

    private EmoServiceMode (Aspect... selected) {
        aspects = EnumSet.noneOf(Aspect.class);
        Collections.addAll(aspects, selected);
    }

    public boolean specifies (Aspect aspect) {
        return aspects.contains(aspect);
    }

    public boolean excludes (Aspect aspect) {
        return !aspects.contains(aspect);
    }

    /**
     * Aspects define granular features / behaviors we need to run per role.
     * Each aspect falls in one of two categories:
     *
     * <ol>
     *     <li>standard:
     *          Used by one of the standard EmoDB services, such as SOR, Queue or Databus</li>
     *     <li>non-standard:
     *          Used exclusively by a role which does not provide a standard EmoDB service, such as the scanner</li>
     * </ol>
     */
    public enum Aspect {
        web,
        task, // Need this if you want to register admin tasks
        cache,
        leader_control, // To get LeaderServiceTask
        background_table_maintenance, // tied to leader aspect
        sor_zookeeper_full_consistency, // Allows for HintsPoller service, and ConsistencyControl tasks
        blob_zookeeper_full_consistency, // Allows for HintsPoller service, and ConsistencyControl tasks
        dataCenter,
        dataCenter_announce,
        dataStore_web, // Only include DataStoreResources
        // Include any DataStore related admin tasks or backend services (unless protected by background_table_maintenance)
        dataStore_module,
        blobStore_web, // Only include BlobStore resources
        // Include any BlobStore related admin tasks or backend services (unless protected by background_table_maintenance)
        blobStore_module,
        dataBus_web,
        dataBus_module,
        dataBus_fan_out_and_replication,
        queue_web,
        queue_module,
        blackList,
        throttle,
        report,
        compaction_control,
        compaction_control_web,
        job,
        full_consistency, // This wires in the fct global zookeeper location
        security,
        invalidation_cache_listener, // This makes sure the node is registered in zookeeper to invalidate its caches
        scanner(false),
        delta_migrator(false),
        swagger,
        uac;

        private boolean _standard;

        Aspect() {
            this(true);
        }

        Aspect(boolean standard) {
            _standard = standard;
        }

        public boolean isStandard() {
            return _standard;
        }

        public static EnumSet<Aspect> getStandardAspects() {
            EnumSet<Aspect> standardAspects = EnumSet.noneOf(Aspect.class);
            for (Aspect aspect : Aspect.values()) {
                if (aspect.isStandard()) {
                    standardAspects.add(aspect);
                }
            }
            return standardAspects;
        }
    }
}
