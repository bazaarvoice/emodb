package com.bazaarvoice.emodb.plugin.util;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.plugin.Plugin;
import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import io.dropwizard.setup.Environment;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Utility for generating and initializing a plugin.
 */
public class PluginInstanceGenerator {

    private PluginInstanceGenerator() {
        // empty
    }

    /**
     * Instantiates a plugin implementation and initializes it.  The configuration class is introspected from
     * the instance type.  The config parameter should be a JSON map-model of the configuration object; this will
     * be converted to the configuration class.  If the plugin does not require any configuration then use {@link Void}
     * as the configuration class.  In this case the configuration object passed to init() will always be null.
     *
     * There are two pre-requisites for this method to work correctly.  First, the initialization class must have a
     * publicly-available no-argument constructor, otherwise it will fail to create an instance.
     *
     * Second, there are certain cases where introspecting the configuration class won't work, notably when the
     * implementation class indirectly declares the configuration class.  For the most part this is uncommon and easily
     * refactored.  For example Sample1's configuration class from the following example will be successfully
     * introspected:
     *
     * <code>
     * public class Sample1 implements ServerStartedListener<ConfigurationType> {
     *     ...
     * }
     * </code>
     *
     * Sample2's configuration class from the following example will fail to be introspected:
     *
     * <code>
     * public class BaseImplementation<T> implements ServerStartedListener<T> {
     *     ...
     * }
     *
     * public class Sample2<ConfigurationType> extends BaseImplementation {
     *     ...
     * }
     * </code>
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public static <T extends Plugin> T generateInstance(String className, Class<T> instanceType, Map<String, Object> config,
                                                        Environment environment, PluginServerMetadata metadata) {
        try {
            requireNonNull(className, "className");
            requireNonNull(instanceType, "instanceType");

            // Verify the class exists
            Class<?> clazz = Class.forName(className);

            // Verify the instance's class implements the required interface.
            checkArgument(instanceType.isAssignableFrom(clazz), "Class {} does not implement or extend {}", clazz, instanceType);

            // Locate the Plugin configuration type.
            Class<?> configClass = null;
            Class<?> c = clazz;
            do {
                for (Type type : c.getGenericInterfaces()) {
                    if (type instanceof ParameterizedType && ((ParameterizedType) type).getRawType().equals(instanceType)) {
                        Type[] actualTypes = ((ParameterizedType) type).getActualTypeArguments();
                        if (actualTypes.length == 1) {   // Sanity check; should always be true
                            Type actualType = actualTypes[0];
                            if (actualType instanceof ParameterizedType) {
                                configClass = (Class<?>) ((ParameterizedType) actualType).getRawType();
                            } else {
                                configClass = (Class<?>) actualType;
                            }
                        }
                    }
                }
                c = c.getSuperclass();
            } while (configClass == null);

            // Attempt to create an instance using a no-argument constructor
            T instance = (T) clazz.newInstance();

            // Get the initialization method.
            Method init = clazz.getMethod("init", Environment.class, PluginServerMetadata.class, configClass);

            Object configImpl = null;
            if (!configClass.equals(Void.class)) {
                // Determine the type for the configuration class
                final Type type = init.getGenericParameterTypes()[2];

                // Use Jackson to convert the configuration map to type
                configImpl = JsonHelper.convert(config, new TypeReference<Object>() {
                    @Override
                    public Type getType() {
                        return type;
                    }
                });
            }

            // Initialize the instance
            instance.init(environment, metadata, configImpl);

            return instance;
        } catch (Throwable t) {
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }
}
