package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;
import com.codahale.metrics.MetricRegistry;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import com.sun.jersey.client.apache4.config.ApacheHttpClient4Config;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import org.apache.http.client.HttpClient;

import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EmoClient implementation that uses a Jersey client.
 */
public class JerseyEmoClient implements EmoClient {

    private static ValidatorFactory _validatorFactory = Validation.buildDefaultValidatorFactory();

    private final Client _client;

    public JerseyEmoClient(Client client) {
        _client = checkNotNull(client, "client");
    }

    @Override
    public EmoResource resource(URI uri) {
        return new JerseyEmoResource(_client.resource(uri));
    }

    public static JerseyEmoClient forHttpConfiguration(HttpClientConfiguration configuration,
                                                                 MetricRegistry metricRegistry, String serviceName) {
        return new JerseyEmoClient(createDefaultJerseyClient(configuration, metricRegistry, serviceName));
    }

    private static ApacheHttpClient4 createDefaultJerseyClient(HttpClientConfiguration configuration, MetricRegistry metricRegistry, String serviceName) {
        HttpClient httpClient = new HttpClientBuilder(metricRegistry).using(configuration).build(serviceName);
        ApacheHttpClient4Handler handler = new ApacheHttpClient4Handler(httpClient, null, true);
        ApacheHttpClient4Config config = new DefaultApacheHttpClient4Config();
        config.getSingletons().add(new JacksonMessageBodyProvider(Jackson.newObjectMapper(), _validatorFactory.getValidator()));
        return new ApacheHttpClient4(handler, config);
    }
}
