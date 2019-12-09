package com.bazaarvoice.shaded.emodb.sor.client;

import com.bazaarvoice.emodb.sor.client.AbstractDataStoreClientFactory;
import com.bazaarvoice.shaded.emodb.dropwizard6.DropWizard6EmoClient;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;

public class DataStoreClientFactory extends AbstractDataStoreClientFactory {

    /**
     * Connects to the System of Record using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DataStoreClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new DataStoreClientFactory(clusterName, client);
    }

    private DataStoreClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new DropWizard6EmoClient(jerseyClient));
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return super.isRetriableException(e) ||
            (e instanceof UniformInterfaceException &&
                ((UniformInterfaceException) e).getResponse().getStatus() >= 500) ||
            Iterables.any(Throwables.getCausalChain(e), Predicates.instanceOf(ClientHandlerException.class));
    }
}
