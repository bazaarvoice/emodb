package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Unique key for identifying a role.  Each key consists of a group and an ID.  It is possible for a role to belong to
 * the reserved "no group" group.  When creating an EmoRoleKey with no group either use the single argument
 * constructor, {@link #EmoRoleKey(String)}, or pass <code>null></code> or {@link #NO_GROUP} as the "group"
 * parameter to {@link #EmoRoleKey(String, String)}.
 */
public class EmoRoleKey {

    public static final String NO_GROUP = "_";

    private final String _group;
    private final String _id;

    public EmoRoleKey(String id) {
        this(NO_GROUP, id);
    }

    @JsonCreator
    public EmoRoleKey(@JsonProperty("group") String group, @JsonProperty("id") String id) {
        _group = Optional.ofNullable(group).orElse(NO_GROUP);
        _id = requireNonNull(id, "id");
    }

    /**
     * Returns the role key's group.  Note that this will never return null; if the role key has no group this method
     * will return {@link #NO_GROUP}.
     */
    public String getGroup() {
        return _group;
    }

    /**
     * Returns the role key's ID.
     */
    public String getId() {
        return _id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EmoRoleKey)) {
            return false;
        }

        EmoRoleKey that = (EmoRoleKey) o;

        return _group.equals(that._group) && _id.equals(that._id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_group, _id);
    }

    public String toString() {
        return _group + "/" + _id;
    }
}
