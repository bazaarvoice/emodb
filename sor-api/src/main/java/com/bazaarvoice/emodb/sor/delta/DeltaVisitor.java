package com.bazaarvoice.emodb.sor.delta;

import javax.annotation.Nullable;

public interface DeltaVisitor<T, V> {

    @Nullable
    V visit(Literal delta, @Nullable T context);

    @Nullable
    V visit(NoopDelta delta, @Nullable T context);

    @Nullable
    V visit(Delete delta, @Nullable T context);

    @Nullable
    V visit(MapDelta delta, @Nullable T context);

    @Nullable
    V visit(SetDelta delta, @Nullable T context);

    @Nullable
    V visit(ConditionalDelta delta, @Nullable T context);
}
