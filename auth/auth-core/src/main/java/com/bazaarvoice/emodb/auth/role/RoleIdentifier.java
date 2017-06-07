package com.bazaarvoice.emodb.auth.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.apache.shiro.authz.AuthorizationInfo;

import javax.annotation.Nullable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Object implementation for the unique identifier for a role.  This ID consists of the role's group and id.
 *
 * Note this this identifier is only used in the context of managing roles.  Shiro represents roles as Strings,
 * as demonstrated by {@link AuthorizationInfo#getRoles()}, so in all authentication and authorization interfaces
 * the string representation as returned by {@link #toString()} is used to identify roles.
 */
public class RoleIdentifier implements Comparable<RoleIdentifier> {
    private final String _group;
    private final String _id;

    @JsonCreator
    public static RoleIdentifier fromString(String idStr) {
        int s = idStr.indexOf('/');
        if (s == -1) {
            // Role has no group
            return new RoleIdentifier(null, idStr);
        }
        // Split by group and role
        return new RoleIdentifier(idStr.substring(0, s), idStr.substring(s+1));
    }

    public RoleIdentifier(@Nullable String group, String id) {
        _group = group;
        _id = checkNotNull(id, "id");
    }

    @Nullable
    public String getGroup() {
        return _group;
    }

    public String getId() {
        return _id;
    }

    @JsonValue
    @Override
    public String toString() {
        if (_group == null) {
            // When a role has no group the string representation is just the id.
            return _id;
        }
        // Since "/" isn't a valid character in groups or ids it can be used as a separator without
        // needing to encode either component.
        return _group + "/" + _id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RoleIdentifier)) {
            return false;
        }

        RoleIdentifier that = (RoleIdentifier) o;

        return _id.equals(that._id) && Objects.equals(_group, that._group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_group, _id);
    }

    @Override
    public int compareTo(RoleIdentifier o) {
        return ComparisonChain.start()
                .compare(getGroup(), o.getGroup(), Ordering.natural().nullsFirst())
                .compare(getId(), o.getId())
                .result();
    }
}
