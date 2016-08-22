package com.bazaarvoice.emodb.web.cli;

import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.google.common.net.HostAndPort;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

public class UnregisterCassandraCommand extends ConfiguredCommand<EmoConfiguration> {
    public UnregisterCassandraCommand() {
        super("unregister-cassandra", "Unregister a manually registered Cassandra host from ZooKeeper.");
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

        ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                .withServiceName(serviceName)
                .withId(host.toString())
                .build();

        String dir = ZKPaths.makePath("ostrich", endPoint.getServiceName());
        String path = ZKPaths.makePath(dir, endPoint.getId());

        curator.newNamespaceAwareEnsurePath(dir).ensure(curator.getZookeeperClient());
        try {
            curator.delete().forPath(path);
            System.out.println("Deleted.");
        } catch (KeeperException.NoNodeException e) {
            System.out.println("Not found.");
        }

        curator.close();
    }
}
