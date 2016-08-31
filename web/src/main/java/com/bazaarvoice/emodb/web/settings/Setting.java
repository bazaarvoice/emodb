package com.bazaarvoice.emodb.web.settings;

import com.google.common.base.Supplier;

import javax.annotation.Nullable;

/**
 * Interface for a dynamic EmoDB setting.  Unlike system configuration which defines immutable attributes of the
 * system a setting should affect system behavior but also be adjustable, usually through an administration task.
 * Settings are system-wide and so affect all EmoDB instances in the cluster across all data centers, although there may
 * be a propagation delay across data centers due to Cassandra replication and local caching of values.  Therefore,
 * settings are best used under the following conditions:
 *
 * <ul>
 *     <li>The setting represents a value which should be tunable on a live system without requiring a restart.
 *     <li>The setting applies universally and is not data center specific.
 *     <li>Changes to the setting are relatively infrequent and eventual propagation to remote data centers
 *         is tolerable.
 * </ul>
 *
 * For example, consider the possibility of using settings for a client throttling feature.  With this feature any
 * client IP making too many requests would be throttled until it backs off to a reasonable request rate.  Storing the
 * actual client IPs being throttled is a poor fit for a setting since they are typically localized to a data center
 * and can change frequently based on an uncontrolled variable (client traffic).  Parameters affecting the
 * <em>behavior</em> of client throttling, such as throttling limits and timeouts, may be good candidates for settings.
 *
 * Consumers who only need to read the setting's value should use the {@link Supplier} interface since this gives them
 * read-only access to the current value.  Tasks responsible for changing a setting's value should use the
 * {@link Setting} interface.
 *
 * @param <T> The setting's value type
 */
public interface Setting<T> extends Supplier<T> {

    /**
     * Returns the name of the setting.
     * @return the name
     */
    String getName();

    /**
     * Updates the current value for the setting.  This updated value will available immediately in the local data
     * center after the method returns; remote data centers will eventually see the updated value, typically in less
     * than one minute.
     */
    void set(@Nullable T value);
}
