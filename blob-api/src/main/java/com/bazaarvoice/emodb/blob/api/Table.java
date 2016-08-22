package com.bazaarvoice.emodb.blob.api;

import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;

@JsonDeserialize(as = DefaultTable.class)
public interface Table {

    String getName();

    TableOptions getOptions();

    Map<String, String> getAttributes();

    TableAvailability getAvailability();
}
