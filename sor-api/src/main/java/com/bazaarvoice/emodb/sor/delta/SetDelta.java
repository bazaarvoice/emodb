package com.bazaarvoice.emodb.sor.delta;

import java.util.Set;

public interface SetDelta {
    boolean getRemoveRest();

    Set<Literal> getAddedValues();

    Set<Literal> getRemovedValues();

    boolean getDeleteIfEmpty();
}
