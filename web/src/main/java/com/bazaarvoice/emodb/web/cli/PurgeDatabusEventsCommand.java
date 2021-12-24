package com.bazaarvoice.emodb.web.cli;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.databus.client.DatabusClient;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.QueueClient;
import com.bazaarvoice.emodb.queue.client.QueueServiceAuthenticator;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import com.sun.jersey.client.apache4.config.ApacheHttpClient4Config;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.http.client.HttpClient;

import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PurgeDatabusEventsCommand extends ConfiguredCommand<EmoConfiguration> {

    private static ValidatorFactory _validatorFactory = Validation.buildDefaultValidatorFactory();

    public PurgeDatabusEventsCommand() {
        super("purge-databus-events", "Purges selected databus events from a subscription based on table/key match.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("--host").required(true).help("Url prefix to connect to (ELB), eg. http://localhost:8080.");
        subparser.addArgument("--limit").required(false).type(Integer.class).setDefault(Integer.MAX_VALUE).help("Maximum number of events to purge.");
        subparser.addArgument("--subscription").required(true).help("Purge events for the specified subscription");
        subparser.addArgument("--table").required(true).nargs("+").help("Purge events for the specified table(s)");
        subparser.addArgument("--key").nargs("+").help("Purge events for the specified key(s)");
        subparser.addArgument("--api-key").required(true).help("API key with privileges to purge the subscription");
    }

    @Override
    protected void run(Bootstrap<EmoConfiguration> bootstrap, Namespace namespace, EmoConfiguration config)
            throws Exception {
        String host = namespace.getString("host");
        int limit = namespace.getInt("limit");
        String subscription = namespace.getString("subscription");
        String apiKey = namespace.getString("api_key");
        Set<String> tables = Sets.newHashSet(namespace.<String>getList("table"));
        List<String> keys = namespace.getList("key");
        Set<String> keySet = keys != null ? Sets.newHashSet(keys) : null;

        System.out.println("Connecting...");

        URI uri = URI.create(host).resolve(DatabusClient.SERVICE_PATH + "/_raw");
        MetricRegistry metricRegistry = bootstrap.getMetricRegistry();
        Client client = createDefaultJerseyClient(config.getHttpClientConfiguration(), metricRegistry, "");

        QueueService databus = QueueServiceAuthenticator.proxied(new QueueClient(uri, new JerseyEmoClient(client)))
                .usingCredentials(apiKey);

        for (;;) {
            List<Message> events = databus.peek(subscription, 5000);

            List<String> ids = Lists.newArrayList();
            for (Message event : events) {
                //noinspection unchecked
                Map<String, String> coord = (Map<String, String>) requireNonNull(event.getPayload());
                String table = requireNonNull(coord.get("table"));
                String key = requireNonNull(coord.get("key"));
                if (tables.contains(table) && (keySet == null || keySet.contains(key))) {
                    ids.add(event.getId());
                    if (--limit <= 0) {
                        break;
                    }
                }
            }
            if (ids.isEmpty()) {
                System.out.println("All matching events of the first " + events.size() + " have been purged.");
                break;
            }

            System.out.println("Purging " + ids.size() + " events...");
            databus.acknowledge(subscription, ids);

            if (limit == 0) {
                System.out.println("Limit reached.");
                break;
            }
        }
    }

    private static ApacheHttpClient4 createDefaultJerseyClient(HttpClientConfiguration configuration, MetricRegistry metricRegistry, String serviceName) {
        HttpClient httpClient = new HttpClientBuilder(metricRegistry).using(configuration).build(serviceName);
        ApacheHttpClient4Handler handler = new ApacheHttpClient4Handler(httpClient, null, true);
        ApacheHttpClient4Config config = new DefaultApacheHttpClient4Config();
        config.getSingletons().add(new JacksonMessageBodyProvider(Jackson.newObjectMapper(), _validatorFactory.getValidator()));
        return new ApacheHttpClient4(handler, config);
    }
}
