package com.bazaarvoice.emodb.streaming;

import java.io.IOException;
import java.io.StringWriter;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.Objects.requireNonNull;

/**
 * Similar to {@link java.util.stream.Collectors#joining()} except instead of producing a new string it appends to
 * a provided {@link Appendable}.
 */
final public class AppendableJoiner {

    private AppendableJoiner() {
        // empty
    }

    public static <T> Collector<T, Appendable, Appendable> joining(final Appendable appendable, final String separator) {
        return joining(appendable, separator, null, null, toStringFunction());
    }

    public static <T> Collector<T, Appendable, Appendable> joining(final Appendable appendable, final String separator,
                                                                   final String prefix, final String suffix) {
        return joining(appendable, separator, prefix, suffix, toStringFunction());
    }

    public static <T> Collector<T, Appendable, Appendable> joining(final Appendable appendable, final String separator,
                                                                   final AppendingFunction<T> appendingFunction) {
        return joining(appendable, separator, null, null, appendingFunction);
    }

    public static <T> Collector<T, Appendable, Appendable> joining(final Appendable appendable, final String separator,
                                                                   final String prefix, final String suffix,
                                                                   final AppendingFunction<T> appendingFunction) {
        requireNonNull(appendable, "appendable");

        return new Collector<T, Appendable, Appendable>() {
            private boolean _first = true;
            private AtomicBoolean _supplyProvidedAppendable = new AtomicBoolean(true);

            @Override
            public Supplier<Appendable> supplier() {
                return () -> {
                    if (_supplyProvidedAppendable.compareAndSet(true, false)) {
                        if (prefix != null) {
                            try {
                                appendable.append(prefix);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return appendable;
                    } else {
                        return new StringWriter();
                    }
                };
            }

            @Override
            public BiConsumer<Appendable, T> accumulator() {
                return (app, t) -> {
                    try {
                        if (app == appendable && _first) {
                            _first = false;
                        } else if (separator != null) {
                            app.append(separator);
                        }
                        appendingFunction.appendTo(app, t);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };
            }

            @Override
            public BinaryOperator<Appendable> combiner() {
                return (app1, app2) -> {
                    try {
                        if (app1 != appendable) {
                            appendable.append(app1.toString());
                        }
                        if (app2 != appendable) {
                            appendable.append(app2.toString());
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return appendable;
                };
            }

            @Override
            public Function<Appendable, Appendable> finisher() {
                return app -> {
                    if (suffix != null) {
                        try {
                            app.append(suffix);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return app;
                };
            }

            @Override
            public Set<Characteristics> characteristics() {
                return EnumSet.noneOf(Characteristics.class);
            }
        };
    }

    public interface AppendingFunction<T> {
        void appendTo(Appendable appendable, T t) throws IOException;
    }

    public static <T> AppendingFunction<T> toStringFunction() {
        return (appendable, t) -> {
            if (t != null) {
                appendable.append(t.toString());
            } else {
                appendable.append("null");
            }
        };
    }
}
