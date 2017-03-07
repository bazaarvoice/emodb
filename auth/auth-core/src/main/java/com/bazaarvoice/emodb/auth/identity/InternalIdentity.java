package com.bazaarvoice.emodb.auth.identity;

import java.util.Date;
import java.util.Set;

/**
 * Interface for getting information about an identity for internal purposes.  Notably it this interface does not
 * expose the secret ID of the identity, such as its API key.  This allows use of the identity for validating
 * permissions and other common metadata without exposing the ID necessary for logging in, spoofing, or otherwise
 * leaking the identity.
 */
public interface InternalIdentity {

    String getInternalId();

    Set<String> getRoles();

    String getOwner();

    String getDescription();

    Date getIssued();

    IdentityState getState();
}
