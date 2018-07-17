package test.integration.queue;

import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.QueueClientFactory;
import com.bazaarvoice.emodb.queue.client.QueueServiceAuthenticator;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.bazaarvoice.ostrich.exceptions.OnlyBadHostsException;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.logging.LoggingFactory;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class QueueStressTest {
    private static final Logger _log = LoggerFactory.getLogger(QueueStressTest.class);

    private static final String QUEUE = "stress";
    private static final int MAX = 50 * 1000 * 1000;

    private final QueueService _queueService;
    private final Semaphore _semaphore = new Semaphore(MAX);
    private final AtomicLong _numWrites = new AtomicLong();
    private final AtomicLong _numReads = new AtomicLong();
    private final AtomicLong _numIdle = new AtomicLong();
    private volatile boolean _done;

    public QueueStressTest(QueueService queueService) {
        _queueService = queueService;
    }

    public void write() {
        Random random = new Random();
        int n = 200;
        while (!_done) {
            try {
                _semaphore.acquire(n);
                List<Object> messages = Lists.newArrayList();
                for (int i = 0; i < n; i++) {
                    messages.add(randomString(random));
                }
                _queueService.sendAll(QUEUE, messages);
                _numWrites.addAndGet(messages.size());

            } catch (OnlyBadHostsException obhe) {
                _log.error("Only bad hosts.  Sleeping a while...");
                sleep(1000);

            } catch (Exception e) {
                _log.error("Exception sending message.", e);
                sleep(100);
            }
        }
    }

    public void read() {
        int n = 100;
        while (!_done) {
            try {
                List<Message> messages = _queueService.poll(QUEUE, Duration.ofSeconds(30), n);
                int count = messages.size();
                if (count == 0) {
                    _numIdle.incrementAndGet();
                    sleep(100);
                    continue;
                }
                List<String> messageIds = Lists.newArrayList();
                for (Message message : messages) {
                    messageIds.add(message.getId());
                }
                _queueService.acknowledge(QUEUE, messageIds);
                _semaphore.release(count);
                _numReads.addAndGet(count);

            } catch (OnlyBadHostsException obhe) {
                _log.error("Only bad hosts.  Sleeping a while...");
                sleep(1000);

            } catch (Exception e) {
                _log.error("Exception polling the queue service.", e);
                sleep(100);
            }
        }
    }

    public void report() {
        long prevWrites = 0, prevReads = 0, prevIdle = 0;
        while (!_done) {
            sleep(1000);
            long numWrites = _numWrites.get();
            long numReads = _numReads.get();
            long numIdle = _numIdle.get();
            int queueSize = MAX - _semaphore.availablePermits();

            _log.info(format("%,5d writes/s;\t%,5d reads/s;\t%,5d idle/s;\t%,8d size",
                    (numWrites - prevWrites),
                    (numReads - prevReads),
                    (numIdle - prevIdle),
                    queueSize));
            prevWrites = numWrites;
            prevReads = numReads;
            prevIdle = numIdle;
        }
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    public void close() {
        _done = true;
    }

    private String randomString(Random random) {
        return new UUID(random.nextLong(), random.nextLong()).toString();
    }

    public static void main(String... args) throws Exception {
        final String DROPWIZARD_PROPERTY_PREFIX = "dw";

        // Load the config.yaml file specified as the first argument.
        ConfigurationFactory<EmoConfiguration> configFactory = new ConfigurationFactory(
                EmoConfiguration.class, Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(), DROPWIZARD_PROPERTY_PREFIX);
        EmoConfiguration configuration = configFactory.build(new File(args[0]));
        int numWriterThreads = Integer.parseInt(args[1]);
        int numReaderThreads = Integer.parseInt(args[2]);
        String adminApiKey = configuration.getAuthorizationConfiguration().getAdminApiKey();

        MetricRegistry metricRegistry = new MetricRegistry();
        new LoggingFactory().configure(metricRegistry, "stress");

        CuratorFramework curator = configuration.getZooKeeperConfiguration().newCurator();
        curator.start();

        QueueClientFactory queueFactory = QueueClientFactory.forClusterAndHttpConfiguration(
                configuration.getCluster(), configuration.getHttpClientConfiguration(), metricRegistry);
        AuthQueueService authQueueService = ServicePoolBuilder.create(AuthQueueService.class)
                .withServiceFactory(queueFactory)
                .withHostDiscovery(new ZooKeeperHostDiscovery(curator, queueFactory.getServiceName(), metricRegistry))
                .withMetricRegistry(metricRegistry)
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
        QueueService queueService = QueueServiceAuthenticator.proxied(authQueueService)
                .usingCredentials(adminApiKey);

        final QueueStressTest stressTest = new QueueStressTest(queueService);

        ThreadFactory writerFactory = new ThreadFactoryBuilder().setNameFormat("Writer-%d").build();
        for (int i = 0; i < numWriterThreads; i++) {
            writerFactory.newThread(new Runnable() {
                @Override
                public void run() {
                    stressTest.write();
                }
            }).start();
        }

        ThreadFactory readerFactory = new ThreadFactoryBuilder().setNameFormat("Reader-%d").build();
        for (int i = 0; i < numReaderThreads; i++) {
            readerFactory.newThread(new Runnable() {
                @Override
                public void run() {
                    stressTest.read();
                }
            }).start();
        }

        ThreadFactory reportFactory = new ThreadFactoryBuilder().setNameFormat("Report-%d").build();
        Executors.newScheduledThreadPool(1, reportFactory).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                stressTest.report();
            }
        }, 1, 1, TimeUnit.SECONDS);

        // Run forever
    }
}
