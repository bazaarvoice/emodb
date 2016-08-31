package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class DatabusSuppressedEventConditionAdminTaskTest {

    @Test
    public void testGetCurrentValue() throws Exception {
        Setting<Condition> setting = mock(Setting.class);
        when(setting.get()).thenReturn(Conditions.alwaysFalse());
        TaskRegistry taskRegistry = mock(TaskRegistry.class);

        DatabusSuppressedEventConditionAdminTask task = new DatabusSuppressedEventConditionAdminTask(setting, taskRegistry);

        StringWriter out = new StringWriter();
        ImmutableMultimap<String, String> params = ImmutableMultimap.of();
        task.execute(params, new PrintWriter(out));

        verify(taskRegistry).addTask(task);
        verify(setting, never()).set(any(Condition.class));
        assertEquals(out.toString(), "alwaysFalse()\n");
    }

    @Test
    public void testUpdateValue() throws Exception {
        Setting<Condition> setting = mock(Setting.class);
        when(setting.get()).thenReturn(Conditions.alwaysFalse());
        TaskRegistry taskRegistry = mock(TaskRegistry.class);

        DatabusSuppressedEventConditionAdminTask task = new DatabusSuppressedEventConditionAdminTask(setting, taskRegistry);

        StringWriter out = new StringWriter();
        ImmutableMultimap<String, String> params = ImmutableMultimap.of("value", "{..,\"~tags\":contains(\"re-etl\")}");
        task.execute(params, new PrintWriter(out));

        verify(taskRegistry).addTask(task);
        verify(setting).set(Conditions.mapBuilder().matches("~tags", Conditions.contains("re-etl")).build());

        // Simplify result by collapsing whitespace
        String actual = out.toString().replaceAll("\\s", " ").replaceAll("\\s{1,}", " ");
        String expected = "Prior value: alwaysFalse() Updated value: {..,\"~tags\":contains(\"re-etl\")} ";
        assertEquals(actual, expected);
    }

    @Test
    public void testSetMultipleValuesFails() throws Exception {
        Setting<Condition> setting = mock(Setting.class);
        when(setting.get()).thenReturn(Conditions.alwaysFalse());
        TaskRegistry taskRegistry = mock(TaskRegistry.class);

        DatabusSuppressedEventConditionAdminTask task = new DatabusSuppressedEventConditionAdminTask(setting, taskRegistry);

        StringWriter out = new StringWriter();
        ImmutableMultimap<String, String> params = ImmutableMultimap.of(
                "value", "{..,\"~tags\":contains(\"tag1\")}", "value", "{..,\"~tags\":contains(\"tag1\")}");

        task.execute(params, new PrintWriter(out));

        verify(taskRegistry).addTask(task);
        // Setting should be un-changed
        verify(setting, never()).set(any(Condition.class));
        assertEquals(out.toString(), "Only one \"value\" parameter can be provided\n");
    }

    @Test
    public void testInvalidConditionFails() throws Exception {
        Setting<Condition> setting = mock(Setting.class);
        when(setting.get()).thenReturn(Conditions.alwaysFalse());
        TaskRegistry taskRegistry = mock(TaskRegistry.class);

        DatabusSuppressedEventConditionAdminTask task = new DatabusSuppressedEventConditionAdminTask(setting, taskRegistry);

        StringWriter out = new StringWriter();
        ImmutableMultimap<String, String> params = ImmutableMultimap.of("value", "{..,\"typo\":here}");
        task.execute(params, new PrintWriter(out));

        verify(taskRegistry).addTask(task);
        verify(setting, never()).set(any(Condition.class));
        // Actual text of Condition parse error may vary in the future; make a minimal check for the expected error text
        assertTrue(out.toString().startsWith("Expected a valid value"));
    }
}
