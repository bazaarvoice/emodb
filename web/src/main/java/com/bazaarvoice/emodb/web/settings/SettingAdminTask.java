package com.bazaarvoice.emodb.web.settings;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base task for updating a setting.  Useful for tasks where a single setting should be viewable or mutable.
 *
 * By default, calling the task with no parameters displays the current value being used by this instance.
 * Providing a single "value" parameter updates the setting to the provided value.
 *
 * @param <T> The value type for the setting used by this task.
 */
abstract public class SettingAdminTask<T> extends Task {

    private final static String VALUE_PARAM = "value";

    private final Setting<T> _setting;

    protected SettingAdminTask(String name, Setting<T> setting) {
        super(name);
        _setting = checkNotNull(setting, "setting");
    }

    /**
     * Converts a String value from the task parameter to a value.
     * @param valueParam The String parameter value
     * @return The setting value
     * @throws InvalidValueException The parameter could to be converted to the correct type.
     */
    abstract protected T toValue(String valueParam) throws InvalidValueException;

    @Override
    public void execute(ImmutableMultimap<String, String> params, PrintWriter out)
            throws Exception {

        T currentValue = _setting.get();
        Collection<String> newValueParam = params.get(VALUE_PARAM);

        if (newValueParam.isEmpty()) {
            // Display current value
            out.println(currentValue);
        } else if (newValueParam.size() != 1) {
            out.println("Only one \"value\" parameter can be provided");
        } else {
            // Update value
            try {
                T newValue = toValue(newValueParam.iterator().next());
                _setting.set(newValue);
                out.println("Prior value:   " + currentValue);
                out.println("Updated value: " + newValue);
            } catch (InvalidValueException e) {
                out.println(e.getMessage());
            }
        }
    }

    protected static class InvalidValueException extends Exception {
        public InvalidValueException(String message) {
            super(message);
        }
    }
}
