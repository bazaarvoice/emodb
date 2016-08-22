package com.bazaarvoice.emodb.sor.delta;

import java.util.Map;

public interface MapDelta extends Delta {
    boolean getRemoveRest();

    Map<String, Delta> getEntries();

    boolean getDeleteIfEmpty();
}
