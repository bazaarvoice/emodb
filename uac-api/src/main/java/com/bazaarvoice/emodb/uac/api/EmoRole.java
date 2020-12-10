package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;


/**
 * Role object for an Emo role.  Each role is uniquely identified by a {@link EmoRoleKey}.  Although it is useful
 * for each role to also have a unique name this is not enforced.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EmoRole {

    private final EmoRoleKey _id;
    private String _name;
    private String _description;
    private Set<String> _permissions = ImmutableSet.of();

    @JsonCreator
    private EmoRole(@JsonProperty("group") String group, @JsonProperty("id") String id) {
        this(new EmoRoleKey(
                Optional.ofNullable(group).orElse(EmoRoleKey.NO_GROUP),
                Objects.requireNonNull(id, "id")));
    }

    public EmoRole(EmoRoleKey id) {
        _id = Objects.requireNonNull(id, "id");
    }

    @JsonProperty("group")
    private String getJsonGroup() {
        return _id.getGroup();
    }

    @JsonProperty("id")
    private String getJsonId() {
        return _id.getId();
    }

    @JsonIgnore
    public EmoRoleKey getId() {
        return _id;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public String getDescription() {
        return _description;
    }

    public void setDescription(String description) {
        _description = description;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Set<String> getPermissions() {
        return _permissions;
    }

    public void setPermissions(Set<String> permissions) {
        _permissions = permissions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EmoRole)) {
            return false;
        }

        EmoRole emoRole = (EmoRole) o;

        return _id.equals(emoRole.getId()) &&
                Objects.equals(_name, emoRole.getName()) &&
                Objects.equals(_description, emoRole.getDescription()) &&
                _permissions.equals(emoRole.getPermissions());
    }

    @Override
    public int hashCode() {
        return _id.hashCode();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EmoRole.class.getSimpleName() + "[", "]")
                .add("id=" + _id)
                .add("name='" + _name + "'")
                .add("description='" + _description + "'")
                .add("permissions=" + _permissions)
                .toString();
    }
}
