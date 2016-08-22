package com.bazaarvoice.ostrich.pool;

import com.bazaarvoice.ostrich.PartitionContext;
import com.google.common.base.Defaults;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;

import java.lang.reflect.Method;

import static org.testng.Assert.assertEquals;

/**
 * Utility for exposing package-private Ostrich methods.
 */
public class OstrichAccessors {

    public static <S> PartitionContextValidator<S> newPartitionContextTest(final Class<S> ifc, final Class<? extends S> impl) {
        final PartitionContextSupplier supplier = new AnnotationPartitionContextSupplier(ifc, impl);
        return new PartitionContextValidator<S>() {
            @Override
            public S expect(final PartitionContext expected) {
                return Reflection.newProxy(ifc, new AbstractInvocationHandler() {
                    @Override
                    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
                        PartitionContext actual = supplier.forCall(method, args);
                        assertEquals(actual, expected, "Expected=" + expected.asMap() + ", Actual=" + actual.asMap());
                        return Defaults.defaultValue(method.getReturnType());
                    }
                });
            }
        };
    }
}
