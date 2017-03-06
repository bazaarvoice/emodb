package com.bazaarvoice.emodb.auth.identity;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AuthIdentityModification is the base class for storing partial updates to an {@link AuthIdentity} as part of a create
 * or update operation using {@link AuthIdentityManager#createIdentity(String, String, AuthIdentityModification)} or
 * {@link AuthIdentityManager#updateIdentity(String, AuthIdentityModification)}, respectively.  Subclasses should
 * override to instantiate a new instance from one of the build methods, as well as add any additional modifiers
 * for attributes specific to the AuthIdentity implementation if necessary.
 */
abstract public class AuthIdentityModification<T extends AuthIdentity> {

    private String _owner;
    private boolean _ownerPresent = false;
    private String _description;
    private boolean _descriptionPresent = false;
    private Set<String> _rolesAdded = Sets.newHashSet();
    private Set<String> _rolesRemoved = Sets.newHashSet();

    public AuthIdentityModification<T> withOwner(String owner) {
        _owner = owner;
        _ownerPresent = true;
        return this;
    }

    public AuthIdentityModification<T> withDescription(String description) {
        _description = description;
        _descriptionPresent = true;
        return this;
    }

    public AuthIdentityModification<T> addRoles(Set<String> roles) {
        checkNotNull(roles, "roles");
        checkArgument(Sets.intersection(roles, _rolesRemoved).isEmpty(), "Cannot add and remove the same role");
        _rolesAdded.addAll(roles);
        return this;
    }

    public AuthIdentityModification<T> addRoles(String... roles) {
        return addRoles(ImmutableSet.copyOf(roles));
    }

    public AuthIdentityModification<T> removeRoles(Set<String> roles) {
        checkNotNull(roles, "roles");
        checkArgument(Sets.intersection(roles, _rolesAdded).isEmpty(), "Cannot add and remove the same role");
        _rolesRemoved.addAll(roles);
        return this;
    }

    public AuthIdentityModification<T> removeRoles(String... roles) {
        return removeRoles(ImmutableSet.copyOf(roles));
    }

    protected boolean isOwnerPresent() {
        return _ownerPresent;
    }

    protected String getOwner() {
        return _owner;
    }

    protected boolean isDescriptionPresent() {
        return _descriptionPresent;
    }

    protected String getDescription() {
        return _description;
    }

    /**
     * Helper method for subclasses which, given a set of roles, returns a new set of roles with all added and removed
     * roles from this modification applied.
     */
    protected Set<String> getUpdatedRolesFrom(Set<String> roles) {
        Set<String> updatedRoles = Sets.newHashSet(roles);
        updatedRoles.addAll(_rolesAdded);
        updatedRoles.removeAll(_rolesRemoved);
        return updatedRoles;
    }

    /**
     * Subclasses should return a new AuthIdentity instance based only on the attributes set in this object with
     * the provided internal ID.  For example:
     *
     * <code>
     *      AuthIdentityImpl identity = createAuthIdentityImplModification()
     *          .withOwner("owner@example.com")
     *          .addRoles("role1")
     *          .buildFrom("123");
     *
     *      assert identity.getInternalId().equals("123");
     *      assert identity.getOwner().equals("owner@example.com");
     *      assert identity.getRoles().equals(ImmtuableSet.of("role1"));
     *      assert identity.getDescription() == null;
     * </code>
     */
    abstract public T buildNew(String internalId);

    /**
     * Subclasses should return a new AuthIdentity instance matching the provided entity plus any modifications
     * set in this object.  For example:
     *
     * <code>
     *      AuthIdentityImpl before = getAuthIdentityImplFromSomewhere();
     *      assert before.getInternalId().equals("123");
     *      assert before.getOwner.equals("owner@example.com");
     *      assert before.getDescription.equals("Old description");
     *      assert before.getRoles().equals(ImmutableSet.of("role1"));
     *
     *      AuthIdentityImpl after = createAuthIdentityImplModification()
     *          .withDescription("New description")
     *          .addRoles("role2")
     *          .removeRoles("role1")
     *          .buildFrom(before);
     *
     *      assert after.getInternalId().equals("123");
     *      assert after.getOwner.equals("owner@example.com");
     *      assert after.getDescription.equals("New description");
     *      assert after.getRoles().equals(ImmutableSet.of("role2"));
     * </code>
     */
    abstract public T buildFrom(T identity);
}
