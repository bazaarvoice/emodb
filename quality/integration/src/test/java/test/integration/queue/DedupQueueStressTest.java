package test.integration.queue;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.client.DedupQueueClientFactory;
import com.bazaarvoice.emodb.queue.client.DedupQueueServiceAuthenticator;
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
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class DedupQueueStressTest {
    private static final Logger _log = LoggerFactory.getLogger(DedupQueueStressTest.class);

    private static final String QUEUE = "stress";
    private static final int MAX = 50 * 1000 * 1000;

    private final DedupQueueService _queueService;
    private final Semaphore _semaphore = new Semaphore(MAX);
    private final AtomicLong _numWrites = new AtomicLong();
    private final AtomicLong _numReads = new AtomicLong();
    private final AtomicLong _numIdle = new AtomicLong();
    private volatile boolean _done;

    public DedupQueueStressTest(DedupQueueService queueService) {
        _queueService = queueService;
    }

    private String queue(int id) {
        return QUEUE + (id % 2);
    }

    public void writeLoop(int id) {
        final String queue = queue(id);
        final int n = 200;
        final Random random = new Random();
        loop(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                write(queue, n, random);
                return true;
            }
        });
    }

    public void readLoop(int id) {
        final String queue = queue(id);
        final int n = 1000;
        loop(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return read(queue, n);
            }
        });
    }

    /** Workers do both writing and reading from the same thread. */
    public void workLoop(int id) {
        final String queue = queue(id);
        final int n = 10;
        final int m = 6;
        final Random random = new Random();
        loop(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                write(queue, n, random);
                read(queue, m);
                read(queue, m);
                return true;
            }
        });
    }

    private void loop(Callable<Boolean> task) {
        while (!_done) {
            try {
                boolean idle = !task.call();

                if (!idle) {
                    sleep(100);
                }
            } catch (OnlyBadHostsException obhe) {
                _log.error("Only bad hosts.  Sleeping a while...");
                sleep(1000);

            } catch (Exception e) {
                _log.error("Exception polling the queue service.", e);
                sleep(100);
            }
        }
    }

    private void write(String queue, int n, Random random) throws Exception {
        _semaphore.acquire(n);
        List<Object> messages = Lists.newArrayList();
        for (int i = 0; i < n; i++) {
            messages.add(randomString(random));
        }
        _queueService.sendAll(queue, messages);
        _numWrites.addAndGet(messages.size());
    }

    private boolean read(String queue, int n) {
        List<Message> messages = _queueService.poll(queue, Duration.standardSeconds(30), n);
        int count = messages.size();
        if (count == 0) {
            _numIdle.incrementAndGet();
            return false;
        }

        List<String> messageIds = Lists.newArrayList();
        for (Message message : messages) {
            messageIds.add(message.getId());
        }
        _queueService.acknowledge(queue, messageIds);
        _semaphore.release(count);
        _numReads.addAndGet(count);
        return false;
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
        int numWorkerThreads = Integer.parseInt(args[3]);
        String adminApiKey = configuration.getAuthorizationConfiguration().getAdminApiKey();

        MetricRegistry metricRegistry = new MetricRegistry();
        new LoggingFactory().configure(metricRegistry, "stress");

        CuratorFramework curator = configuration.getZooKeeperConfiguration().newCurator();
        curator.start();

        JerseyEmoClient jerseyEmoClient = JerseyEmoClient.forHttpConfiguration(configuration.getHttpClientConfiguration(),
                metricRegistry, DedupQueueClientFactory.getServiceName(configuration.getCluster()));

        DedupQueueClientFactory queueFactory = DedupQueueClientFactory.forClusterAndEmoClient(
                configuration.getCluster(), jerseyEmoClient);
        AuthDedupQueueService secureQueueService = ServicePoolBuilder.create(AuthDedupQueueService.class)
                .withServiceFactory(queueFactory)
                .withHostDiscovery(new ZooKeeperHostDiscovery(curator, queueFactory.getServiceName(), metricRegistry))
                .withMetricRegistry(metricRegistry)
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
        DedupQueueService queueService = DedupQueueServiceAuthenticator.proxied(secureQueueService)
                .usingCredentials(adminApiKey);

        final DedupQueueStressTest stressTest = new DedupQueueStressTest(queueService);

        ThreadFactory writerFactory = new ThreadFactoryBuilder().setNameFormat("Writer-%d").build();
        for (int i = 0; i < numWriterThreads; i++) {
            final int id = i;
            writerFactory.newThread(new Runnable() {
                @Override
                public void run() {
                    stressTest.writeLoop(id);
                }
            }).start();
        }

        ThreadFactory readerFactory = new ThreadFactoryBuilder().setNameFormat("Reader-%d").build();
        for (int i = 0; i < numReaderThreads; i++) {
            final int id = i;
            readerFactory.newThread(new Runnable() {
                @Override
                public void run() {
                    stressTest.readLoop(id);
                }
            }).start();
        }

        ThreadFactory workerFactory = new ThreadFactoryBuilder().setNameFormat("Worker-%d").build();
        for (int i = 0; i < numWorkerThreads; i++) {
            final int id = i;
            workerFactory.newThread(new Runnable() {
                @Override
                public void run() {
                    stressTest.workLoop(id);
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
