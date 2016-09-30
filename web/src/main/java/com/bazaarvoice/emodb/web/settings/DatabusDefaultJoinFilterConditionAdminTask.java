package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.databus.DefaultJoinFilter;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.deser.ParseException;
import com.google.inject.Inject;

/**
 * Task for managing the condition used to suppress databus events when a subscription opts to ignore suppressed events.
 *
 * Examples:
 *
 * View the current condition:
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/databus-default-join-filter-condition"
 * </code>
 *
 * Update the condition condition:
 * <code>
 *     $ curl -XPOST 'localhost:8081/tasks/databus-default-join-filter-condition?value=\{..,"~tags":contains("re-etl")\}'
 * </code>
 */
public class DatabusDefaultJoinFilterConditionAdminTask extends SettingAdminTask {

    @Inject
    public DatabusDefaultJoinFilterConditionAdminTask(@DefaultJoinFilter Setting<Condition> setting,
                                                      TaskRegistry taskRegistry) {
        super("databus-default-join-filter-condition", setting, DatabusDefaultJoinFilterConditionAdminTask::toCondition);
        taskRegistry.addTask(this);
    }

    private static Condition toCondition(String valueParam) {
        try {
            return Conditions.fromString(valueParam);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}
