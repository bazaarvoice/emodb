package com.bazaarvoice.emodb.web.compactioncontrol.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.client.AbstractDataStoreClientFactoryBase;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.sun.jersey.api.client.Client;

/**
 * SOA factory for Jersey clients to use Compaction control resources.
 */
public class CompactionControlClientFactory extends AbstractDataStoreClientFactoryBase<CompactionControlSource> {

    private final String _apiKey;

    public static CompactionControlClientFactory forClusterAndHttpClient(String clusterName, Client client, String apiKey) {
        return new CompactionControlClientFactory(clusterName, new JerseyEmoClient(client), apiKey);
    }

    public CompactionControlClientFactory(String clusterName, EmoClient client, String apiKey) {
        super(clusterName, client);
        _apiKey = apiKey;
    }

    @Override
    public CompactionControlSource create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        return new CompactionControlClient(payload.getServiceUrl(), _client, _apiKey);
    }
}

