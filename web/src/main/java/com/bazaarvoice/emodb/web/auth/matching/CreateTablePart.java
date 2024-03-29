package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.Implier;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This part is used for verifying permissions for creating a new table by other permissions.  As such no permissions
 * are implied by this permission.
 */
public class CreateTablePart extends EmoMatchingPart {

    private final String _name;
    private final String _placement;
    private final Map<String, ?> _attributes;

    @JsonCreator
    public CreateTablePart(@JsonProperty("name") String name, @JsonProperty("placement") String placement,
                           @JsonProperty("attributes") Map<String, ?> attributes) {
        _name = requireNonNull(name, "name");
        _placement = requireNonNull(placement, "placement");
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
    protected boolean impliedBy(Implier implier, List<MatchingPart> leadingParts) {
        return ((EmoImplier) implier).impliesCreateTable(this, leadingParts);
    }

    /**
     * CreateTablePart is only used to validate whether the parameters for creating a new table are implied in the
     * context of another permission.  It should not be assigned to any users or roles.
     */
    @Override
    public boolean isAssignable() {
        return false;
    }
}
