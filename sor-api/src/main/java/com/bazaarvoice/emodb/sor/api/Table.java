package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;

@JsonDeserialize(as = DefaultTable.class)
public interface Table {

    String getName();

    TableOptions getOptions();

    Map<String, Object> getTemplate();

    TableAvailability getAvailability();
}
