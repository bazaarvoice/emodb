package com.bazaarvoice.emodb.auth.identity;

/**
 * State for an {@link AuthIdentity}.  Currently there are 3 possible values:
 *
 * <ol>
 *     <li>ACTIVE.  This is the normal state for an identity.</li>
 *     <li>INACTIVE.  The identity exists but cannot be authenticated or authorized for any operations.</li>
 *     <li>MIGRATED.  The identity's ID has been migrated and the current identity is a historical record from the
 *                    old ID.  Like INACTIVE, a MIGRATED identity cannot be authenticated or authorized.</li>
 * </ol>
 */
public enum IdentityState {
    ACTIVE,
    INACTIVE,
    MIGRATED;

    /**
     * Returns true if an identity is in a state where it can be authorized or authenticated.  The current
     * implementation redundantly returns true only for ACTIVE.  Even so, use of this method is preferred to
     * verify if an identity can be authorized or authenticated since that tautology may change if other states
     * are introduced.
     */
    public boolean isActive() {
        return this == ACTIVE;
    }
}
