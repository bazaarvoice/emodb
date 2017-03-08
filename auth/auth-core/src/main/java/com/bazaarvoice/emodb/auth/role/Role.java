package com.bazaarvoice.emodb.auth.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * POJO representation of a role.  Note that permissions are not included in this object since the attributes of a role
 * are orthogonal to management of permissions associated with that role.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Role {

    private final String _group;
    private final String _id;
    private String _name;
    private String _description;

    @JsonCreator
    public Role(@Nullable @JsonProperty("group") String group,
                @JsonProperty("id") String id,
                @Nullable @JsonProperty("name") String name,
                @Nullable @JsonProperty("description") String description) {
        _group = group;
        _id = checkNotNull(id, "id");
        _name = name;
        _description = description;
    }

    @JsonIgnore
    public RoleIdentifier getRoleIdentifier() {
        return new RoleIdentifier(_group, _id);
    }
    
    @Nullable
    public String getGroup() {
        return _group;
    }

    public String getId() {
        return _id;
    }
    
    @Nullable
    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    @Nullable
    public String getDescription() {
        return _description;
    }

    public void setDescription(String description) {
        _description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Role)) {
            return false;
        }

        Role that = (Role) o;

        return _id.equals(that._id) &&
                _name.equals(that._name) &&
                Objects.equals(_group, that._group) &&
                Objects.equals(_description, that._description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_group, _id);
    }
}
