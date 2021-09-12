package com.bazaarvoice.emodb.web.settings;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class DatabusDefaultJoinFilterConditionAdminTaskTest {

    @Test
    public void testGetCurrentValue() throws Exception {
        Setting<Condition> setting = mock(Setting.class);
        when(setting.get()).thenReturn(Conditions.alwaysTrue());
        TaskRegistry taskRegistry = mock(TaskRegistry.class);

        DatabusDefaultJoinFilterConditionAdminTask task = new DatabusDefaultJoinFilterConditionAdminTask(setting, taskRegistry);

        StringWriter out = new StringWriter();
        ImmutableMultimap<String, String> params = ImmutableMultimap.of();
        task.execute(params, new PrintWriter(out));

        verify(taskRegistry).addTask(task);
        verify(setting, never()).set(any(Condition.class));
        assertEquals(out.toString(), "value\n\talwaysTrue()\n");
    }

    @Test
    public void testUpdateValue() throws Exception {
        Setting<Condition> setting = mock(Setting.class);
        when(setting.get()).thenReturn(Conditions.alwaysTrue());
        TaskRegistry taskRegistry = mock(TaskRegistry.class);

        DatabusDefaultJoinFilterConditionAdminTask task = new DatabusDefaultJoinFilterConditionAdminTask(setting, taskRegistry);

        StringWriter out = new StringWriter();
        ImmutableMultimap<String, String> params = ImmutableMultimap.of("value", "not({..,\"~tags\":contains(\"re-etl\")})");
        task.execute(params, new PrintWriter(out));

        verify(taskRegistry).addTask(task);
        verify(setting).set(Conditions.not(
                Conditions.mapBuilder().matches("~tags", Conditions.contains("re-etl")).build()));

        // Simplify result by collapsing whitespace
        String actual = out.toString().replaceAll("\\s", " ").replaceAll("\\s{1,}", " ");
        String expected = "value Prior value: alwaysTrue() Updated value: not({..,\"~tags\":contains(\"re-etl\")}) ";
        assertEquals(actual, expected);
    }

    @Test
    public void testSetMultipleValuesFails() throws Exception {
        Setting<Condition> setting = mock(Setting.class);
        when(setting.get()).thenReturn(Conditions.alwaysTrue());
        TaskRegistry taskRegistry = mock(TaskRegistry.class);

        DatabusDefaultJoinFilterConditionAdminTask task = new DatabusDefaultJoinFilterConditionAdminTask(setting, taskRegistry);

        StringWriter out = new StringWriter();
        ImmutableMultimap<String, String> params = ImmutableMultimap.of(
                "value", "not({..,\"~tags\":contains(\"tag1\")})", "value", "not({..,\"~tags\":contains(\"tag1\")})");

        task.execute(params, new PrintWriter(out));

        verify(taskRegistry).addTask(task);
        // Setting should be un-changed
        verify(setting, never()).set(any(Condition.class));
        assertEquals(out.toString(), "value\n\tOnly one \"value\" parameter can be provided\n");
    }

    @Test
    public void testInvalidConditionFails() throws Exception {
        Setting<Condition> setting = mock(Setting.class);
        when(setting.get()).thenReturn(Conditions.alwaysFalse());
        TaskRegistry taskRegistry = mock(TaskRegistry.class);

        DatabusDefaultJoinFilterConditionAdminTask task = new DatabusDefaultJoinFilterConditionAdminTask(setting, taskRegistry);

        StringWriter out = new StringWriter();
        ImmutableMultimap<String, String> params = ImmutableMultimap.of("value", "not({..,\"typo\":here})");
        task.execute(params, new PrintWriter(out));

        verify(taskRegistry).addTask(task);
        verify(setting, never()).set(any(Condition.class));
        // Actual text of Condition parse error may vary in the future; make a minimal check for the expected error text
        assertTrue(out.toString().contains("Expected a valid value"));
    }
}
