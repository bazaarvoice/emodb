package com.bazaarvoice.emodb.web.cli;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.ServiceEndPointJsonCodec;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException;

import java.util.Date;
import java.util.Map;

import static java.lang.String.format;

public class RegisterCassandraCommand extends ConfiguredCommand<EmoConfiguration> {
    public RegisterCassandraCommand() {
        super("register-cassandra", "Register a Cassandra host manually in ZooKeeper.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("--host").required(true).help("Cassandra host name as <host>:<port>");
        subparser.addArgument("--service").required(true).help("Ostrich service name");
    }

    @Override
    protected void run(Bootstrap<EmoConfiguration> bootstrap, Namespace namespace, EmoConfiguration configuration)
            throws Exception {
        String hostString = namespace.getString("host");
        String serviceName = namespace.getString("service");

        CuratorFramework curator = configuration.getZooKeeperConfiguration().newCurator();
        curator.start();

        HostAndPort host = HostAndPort.fromString(hostString).withDefaultPort(9160);

        // Validate arguments.  Verify that Cassandra is listening at the expected host/port and get the partitioner.
        String partitioner = null;
        try {
            TTransport tr = new TFramedTransport(new TSocket(host.getHostText(), host.getPort()));
            Cassandra.Client client = new Cassandra.Client(new TBinaryProtocol(tr));
            tr.open();
            partitioner = client.describe_partitioner();
            tr.close();
        } catch (Exception e) {
            System.err.println(format("Unable to connect to '%s': %s", host, e));
            System.exit(1);
        }

        // Fake out an Ostrich service registration.
        ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                .withServiceName(serviceName)
                .withId(host.toString())
                .withPayload(JsonHelper.asJson(ImmutableMap.of("partitioner", partitioner)))
                .build();
        Map<String, Object> registrationData = ImmutableMap.<String, Object>of(
                "registration-time", JsonHelper.formatTimestamp(new Date()));
        byte[] data = ServiceEndPointJsonCodec.toJson(endPoint, registrationData).getBytes(Charsets.UTF_8);

        String path = ZKPaths.makePath(ZKPaths.makePath("ostrich", endPoint.getServiceName()), endPoint.getId());

        // Create a regular (not ephemeral) node in ZooKeeper for the registration.
        try {
            curator.create().creatingParentsIfNeeded().forPath(path, data);
            System.out.println("Created.");
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("Already exists.");
        }

        curator.close();
    }
}
