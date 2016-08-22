package com.bazaarvoice.emodb.web.cli;

import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.curator.framework.CuratorFramework;

public class ListCassandraCommand extends ConfiguredCommand<EmoConfiguration> {
    public ListCassandraCommand() {
        super("list-cassandra", "List Cassandra hosts registered in ZooKeeper.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("--service").required(true).help("Ostrich service name");
    }

    @Override
    protected void run(Bootstrap<EmoConfiguration> bootstrap, Namespace namespace, EmoConfiguration configuration)
            throws Exception {
        String serviceName = namespace.getString("service");

        CuratorFramework curator = configuration.getZooKeeperConfiguration().newCurator();
        curator.start();

        ZooKeeperHostDiscovery hostDiscovery = new ZooKeeperHostDiscovery(curator, serviceName, bootstrap.getMetricRegistry());

        for (ServiceEndPoint endPoint : hostDiscovery.getHosts()) {
            System.out.println(endPoint.getId());
        }

        hostDiscovery.close();
        curator.close();
    }
}
