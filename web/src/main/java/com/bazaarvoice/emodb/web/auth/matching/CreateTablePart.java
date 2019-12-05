package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.Implier;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

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
