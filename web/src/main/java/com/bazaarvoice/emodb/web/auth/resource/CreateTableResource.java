package com.bazaarvoice.emodb.web.auth.resource;

import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


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
        _name = Objects.requireNonNull(name, "name");
        _placement = Objects.requireNonNull(placement, "placement");
        _attributes = Optional.ofNullable(attributes).orElse(Collections.EMPTY_MAP);
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
