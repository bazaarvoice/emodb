package com.bazaarvoice.emodb.sor.delta;

import javax.annotation.Nullable;

public interface Literal extends Delta, Comparable<Literal> {
    @Nullable
    Object getValue();
}
