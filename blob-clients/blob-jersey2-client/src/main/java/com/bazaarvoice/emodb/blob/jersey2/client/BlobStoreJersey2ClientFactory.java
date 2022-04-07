package com.bazaarvoice.emodb.blob.jersey2.client;

import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import java.net.URI;


public class BlobStoreJersey2ClientFactory extends AbstractBlobStoreJersey2ClientFactory {

    /**
     * Connects to the Blob Store using the specified Jersey client.  If you're using Dropwizard, use this
     * constructor and pass the Dropwizard-constructed Jersey client.
     */
    public static BlobStoreJersey2ClientFactory forClusterAndHttpClient(Client client, URI endpoint) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new BlobStoreJersey2ClientFactory(client, endpoint);
    }

    private BlobStoreJersey2ClientFactory(Client jerseyClient, URI endpoint) {
        super(new Jersey2EmoClient(jerseyClient), endpoint);
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return super.isRetriableException(e) ||
                (e instanceof WebApplicationException &&
                        ((WebApplicationException) e).getResponse().getStatus() >= 500) ||
                Iterables.any(Throwables.getCausalChain(e), Predicates.instanceOf(ProcessingException.class));
    }

    public BlobStoreJersey2ClientFactory withRetry(int maxNumberofAttempts, long baseSleepTime, long maxSleepTime) {
        super.withRetry(maxNumberofAttempts, baseSleepTime, maxSleepTime);
        return this;

    }
}
