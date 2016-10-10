package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.db.cql.CqlForMultiGets;
import com.bazaarvoice.emodb.sor.db.cql.CqlForScans;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

/**
 * Task for managing whether to use the CQL or Astyanax driver.
 *
 * Examples:
 *
 * View the current condition:
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/sor-cql-driver"
 * </code>
 *
 * Set the driver for scans to CQL:
 * <code>
 *     $ curl -XPOST 'localhost:8081/tasks/sor-cql-driver?use-cql-for-scans=true'
 * </code>
 *
 * Set the driver for multi-gets and scans to Astyanax:
 * <code>
 *     $ curl -XPOST 'localhost:8081/tasks/sor-cql-driver?use-cql-for-multi-gets=false&use-cql-for-scans=false'
 * </code>
 */
public class SorCqlDriverTask extends SettingAdminTask {
    @Inject
    public SorCqlDriverTask(TaskRegistry taskRegistry,
                            @CqlForMultiGets Setting<Boolean> useCqlForMultiGetsSetting,
                            @CqlForScans Setting<Boolean> useCqlForScansSetting) {
        super("sor-cql-driver", ImmutableMap.of(
                "use-cql-for-multi-gets", new Settable<>(useCqlForMultiGetsSetting, Boolean::parseBoolean),
                "use-cql-for-scans", new Settable<>(useCqlForScansSetting, Boolean::parseBoolean)));

        taskRegistry.addTask(this);
    }
}
