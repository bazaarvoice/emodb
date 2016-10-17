package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.Databus;

/**
 * Provider similar to {@link com.bazaarvoice.emodb.databus.core.DatabusFactory} that is intended for use by the
 * controller resource to return a Databus owned by the {@link Subject} provided by Jersey.  This is necessary
 * because requests routed internally are authorized using the internal ID while requests routed to another server
 * for partitioning reasons are authorized using the API key.
 */
public interface DatabusClientSubjectProxy {

    Databus forSubject(Subject subject);
}
