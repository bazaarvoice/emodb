package com.bazaarvoice.emodb.web.auth.resource;

import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Resource implementation specifically for validating the creation of new tables.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateTableResource extends AuthResource {

    private final String _name;
    private final String _placement;
    private final Map<String, ?> _attributes;

    @JsonCreator
    public CreateTableResource(@JsonProperty("name") String name, @JsonProperty("placement") String placement,
                               @JsonProperty("attributes") Map<String, ?> attributes) {
        _name = checkNotNull(name, "name");
        _placement = checkNotNull(placement, "placement");
        _attributes = MoreObjects.firstNonNull(attributes, ImmutableMap.<String, Object>of());
    }

    public String getName() {
        return _name;
    }

    public String getPlacement() {
        return _placement;
    }

    public Map<String, ?> getAttributes() {
        return _attributes;
    }

    @Override
    public String toString() {
        return "createTable(" + RisonHelper.asORison(this) + ")";
    }
}
