package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.databus.SuppressedEventCondition;
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
 *     $ curl -XPOST "localhost:8081/tasks/databus-suppressed-event-condition"
 * </code>
 *
 * Update the condition condition:
 * <code>
 *     $ curl -XPOST 'localhost:8081/tasks/databus-suppressed-event-condition?value=\{..,"~tags":contains("re-etl")\}'
 * </code>
 */
public class DatabusSuppressedEventConditionAdminTask extends SettingAdminTask<Condition> {

    @Inject
    public DatabusSuppressedEventConditionAdminTask(@SuppressedEventCondition Setting<Condition> setting,
                                                    TaskRegistry taskRegistry) {
        super("databus-suppressed-event-condition", setting);
        taskRegistry.addTask(this);
    }

    @Override
    protected Condition toValue(String valueParam) throws InvalidValueException {
        try {
            return Conditions.fromString(valueParam);
        } catch (ParseException e) {
            throw new InvalidValueException(e.getMessage());
        }
    }
}
