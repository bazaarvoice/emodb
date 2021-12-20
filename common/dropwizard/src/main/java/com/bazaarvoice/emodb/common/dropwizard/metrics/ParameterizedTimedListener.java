package com.bazaarvoice.emodb.common.dropwizard.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import java.lang.reflect.Method;

import static java.util.Objects.requireNonNull;

/**
 * Guice type listener that looks for methods annotated with {@link ParameterizedTimed} and tracks their
 * performance using the Yammer Metrics library.  This class is different from
 * {@link com.codahale.metrics.jersey.InstrumentedResourceMethodDispatchProvider.TimedRequestDispatcher} in that the
 * metrics group name is provided by the type listener and not by the annotation.
 */
public class ParameterizedTimedListener implements TypeListener {
    private final String _group;
    private final MetricRegistry _metricRegistry;

    public ParameterizedTimedListener(String group, MetricRegistry metricRegistry) {
        _group = requireNonNull(group, "group");
        _metricRegistry = metricRegistry;
    }

    @Override
    public <I> void hear(TypeLiteral<I> literal, TypeEncounter<I> encounter) {
        Class<? super I> type = literal.getRawType();
        for (Method method : type.getMethods()) {
            ParameterizedTimed annotation = method.getAnnotation(ParameterizedTimed.class);
            if (annotation == null) {
                continue;
            }

            String metricType = annotation.type();
            if(metricType == null || metricType.isEmpty()) {
                metricType = type.getSimpleName().replaceAll("\\$$", "");
            }
            String metricName = annotation.name();
            if(metricName == null || metricName.isEmpty()) {
                metricName = method.getName();
            }
            String metric = MetricRegistry.name(_group, metricType, metricName);
            final Timer timer = _metricRegistry.timer(metric);

            encounter.bindInterceptor(Matchers.only(method), new MethodInterceptor() {
                @Override
                public Object invoke(MethodInvocation invocation) throws Throwable {
                    Timer.Context time = timer.time();
                    try {
                        return invocation.proceed();
                    } finally {
                        time.stop();
                    }
                }
            });
        }
    }
}
