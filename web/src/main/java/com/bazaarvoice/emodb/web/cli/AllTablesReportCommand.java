package com.bazaarvoice.emodb.web.cli;

import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.table.db.astyanax.TableUuidFormat;
import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.EmoModule;
import com.bazaarvoice.emodb.web.report.AllTablesReportGenerator;
import com.bazaarvoice.emodb.web.report.AllTablesReportOptions;
import com.bazaarvoice.emodb.web.report.AllTablesReportResult;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.eclipse.jetty.util.component.ContainerLifeCycle;

import java.util.List;

public class AllTablesReportCommand extends EnvironmentCommand<EmoConfiguration> {

    public AllTablesReportCommand() {
        super(NoOpService.<EmoConfiguration>create(), "all-tables-report", "Scans all Cassandra tables and stores the results in a report");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);

        subparser.addArgument("id")
                .required(true)
                .help("IDs for the report (IDs can be reused to continue a partially completed report)");

        subparser.addArgument("--placements")
                .dest("placements")
                .metavar("PLACEMENT")
                .nargs("+")
                .help("Limit report to the provided placements (by default all placements are included)");

        ArgumentGroup continuation = subparser.addArgumentGroup("continue")
                .description("Continue report from a specific shard and table");

        continuation.addArgument("--shard")
                .dest("shard")
                .type(Integer.class)
                .help("The shard ID to continue from (by default the report starts at the first shard)");

        continuation.addArgument("--table")
                .dest("table")
                .help("The UUID of the table to continue from (by default the report starts at the " +
                        "first table in the initial shard");

        subparser.addArgument("--readOnly")
                .action(new StoreTrueArgumentAction())
                .dest("readOnly")
                .help("Do not modify any data, such as fixing invalid compaction records");
    }

    @Override
    protected void run(Environment environment, Namespace namespace, EmoConfiguration configuration)
            throws Exception {
        String id = namespace.getString("id");

        AllTablesReportOptions options = new AllTablesReportOptions();
        options.setReadOnly(namespace.getBoolean("readOnly"));

        List<String> placements = namespace.getList("placements");
        if (placements != null && !placements.isEmpty()) {
            options.setPlacements(ImmutableSet.copyOf(placements));
        }

        Integer shardId = namespace.getInt("shard");
        if (shardId != null) {
            options.setFromShardId(shardId);
        }

        String table = namespace.getString("table");
        if (table != null) {
            options.setFromTableUuid(TableUuidFormat.decode(table));
        }

        Injector injector = Guice.createInjector(new EmoModule(configuration, environment, EmoServiceMode.CLI_TOOL));

        ContainerLifeCycle containerLifeCycle = new ContainerLifeCycle();
        environment.lifecycle().attach(containerLifeCycle);
        containerLifeCycle.start();
        try {
            AllTablesReportGenerator generator = injector.getInstance(AllTablesReportGenerator.class);
            AllTablesReportResult result = generator.runReport(id, options);

            System.out.println(result);
        } finally {
            containerLifeCycle.stop();
        }
    }
}
