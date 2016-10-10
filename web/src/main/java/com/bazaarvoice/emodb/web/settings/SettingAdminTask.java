package com.bazaarvoice.emodb.web.settings;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Base task for updating settings.  Useful for tasks where one or more related setting should be viewable or mutable.
 *
 * By default, calling the task with no parameters displays the current values being used by this instance.
 */
abstract public class SettingAdminTask extends Task {

    private final Map<String, Settable<?>> _settables;

    protected <T> SettingAdminTask(String name, Setting<T> setting, Function<String, T> valueFunction) {
        this(name, ImmutableMap.of("value", new Settable<>(setting, valueFunction)));
    }

    protected SettingAdminTask(String name, Map<String, Settable<?>> settables) {
        super(name);
        _settables = new TreeMap<>(settables);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> params, PrintWriter out)
            throws Exception {

        Map<String, Object> currentValues = ImmutableMap.copyOf(
                Maps.transformValues(_settables, settable -> settable.getSetting().get()));

        for (Map.Entry<String, Settable<?>> entry : _settables.entrySet()) {
            String valueParam = entry.getKey();
            Settable<?> settable = entry.getValue();

            out.println(valueParam);
            Collection<String> newValueParam = params.get(valueParam);
            if (newValueParam.isEmpty()) {
                // Display current value
                out.println("\t" + currentValues.get(valueParam));
            } else if (newValueParam.size() != 1) {
                out.println("\tOnly one \"" + valueParam + "\" parameter can be provided");
            } else {
                // Update value
                try {
                    Object newValue = settable.update(newValueParam.iterator().next());
                    out.println("\tPrior value:   " + currentValues.get(valueParam));
                    out.println("\tUpdated value: " + newValue);
                } catch (Exception e) {
                    out.println(e.getMessage());
                }
            }
        }
    }

    protected final static class Settable<T> {
        private final Setting<T> _setting;
        private final Function<String, T> _valueFunction;

        public Settable(Setting<T> setting, Function<String, T> valueFunction) {
            _setting = setting;
            _valueFunction = valueFunction;
        }

        public Setting<T> getSetting() {
            return _setting;
        }

        public T update(String string) {
            T newValue = _valueFunction.apply(string);
            _setting.set(newValue);
            return newValue;
        }
    }
}
