package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.core.DatabusFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link DatabusClientSubjectProxy} implementation that uses the Subject's internal ID to proxy a DatabusFactory.
 */
public class LocalDatabusClientSubjectProxy implements DatabusClientSubjectProxy {

    private final DatabusFactory _databusFactory;

    public LocalDatabusClientSubjectProxy(DatabusFactory databusFactory) {
        _databusFactory = checkNotNull(databusFactory, "databusFactory");
    }

    @Override
    public Databus forSubject(Subject subject) {
        // Get a Databus instance using the subject's internal ID
        return _databusFactory.forOwner(subject.getInternalId());
    }
}
