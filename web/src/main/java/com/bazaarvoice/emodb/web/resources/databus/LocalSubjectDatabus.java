package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.core.DatabusFactory;

import static java.util.Objects.requireNonNull;

/**
 * SubjectDatabus implementation which forwards requests to a local Databus using the ID as the authenticator.
 */
public class LocalSubjectDatabus extends AbstractSubjectDatabus {

    private final DatabusFactory _databusFactory;

    public LocalSubjectDatabus(DatabusFactory databusFactory) {
        _databusFactory = requireNonNull(databusFactory, "databusFactory");
    }

    @Override
    protected Databus databus(Subject subject) {
        return _databusFactory.forOwner(subject.getId());
    }
}
