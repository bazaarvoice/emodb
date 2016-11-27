package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * SubjectDatabus implementation which forwards calls to an {@link AuthDatabus} using the API key as the authenticator.
 */
public class SubjectDatabusClient extends AbstractSubjectDatabus {

    private final AuthDatabus _authDatabus;
    private final DatabusAuthenticator _authenticator;

    public SubjectDatabusClient(AuthDatabus authDatabus) {
        _authDatabus = checkNotNull(authDatabus, "authDatabus");
        _authenticator = DatabusAuthenticator.proxied(_authDatabus);
    }

    @Override
    protected Databus databus(Subject subject) {
        return _authenticator.usingCredentials(subject.getId());
    }

    AuthDatabus getClient() {
        return _authDatabus;
    }
}
