package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dropwizard provider of the cache invalidation task.
 * <p>
 * Invoke this task as follows:
 * <pre>
 *   curl http://localhost:8081/tasks/invalidate?cache=xyz&scope=local&key=123
 * </pre>
 */
public class DropwizardInvalidationTask extends Task {

    public static final String NAME = "invalidate";

    private final CacheRegistry _registry;

    @Inject
    public DropwizardInvalidationTask(TaskRegistry tasks, CacheRegistry registry) {
        super(NAME);
        _registry = registry;
        tasks.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) {
        String name = checkNotNull(Iterables.getFirst(parameters.get("cache"), null), "cache");
        String scopeString = Iterables.getFirst(parameters.get("scope"), "LOCAL");
        InvalidationScope scope = InvalidationScope.valueOf(scopeString.toUpperCase());
        Collection<String> keys = parameters.get("key");
        boolean invalidateAll = keys.isEmpty();

        CacheHandle handle = _registry.lookup(name, false);
        if (handle == null) {
            out.println("Cache not found: " + name);
            return;
        }

        if (invalidateAll) {
            handle.invalidateAll(scope);
        } else {
            handle.invalidateAll(scope, keys);
        }

        out.println("Done!");
    }
}
