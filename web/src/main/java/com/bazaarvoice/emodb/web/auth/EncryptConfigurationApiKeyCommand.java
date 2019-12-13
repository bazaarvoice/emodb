package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.google.common.base.MoreObjects;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * Command for encrypting API keys which must go into config.yaml.
 */
public class EncryptConfigurationApiKeyCommand extends ConfiguredCommand<EmoConfiguration> {

    public EncryptConfigurationApiKeyCommand() {
        super("encrypt-configuration-api-key", "Encrypts API keys for inclusion in Emo's config.yaml");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("--api-key")
                .required(true)
                .help("The API key to encrypt");
        subparser.addArgument("--cluster")
                .help("The cluster name (defaults to the \"cluster\" attribute from config.yaml)");
    }

    @Override
    protected void run(Bootstrap<EmoConfiguration> bootstrap, Namespace namespace, EmoConfiguration configuration)
            throws Exception {
        String apiKey = namespace.getString("api_key");
        String cluster = MoreObjects.firstNonNull(namespace.getString("cluster"), configuration.getCluster());

        ApiKeyEncryption encryption = new ApiKeyEncryption(cluster);
        System.out.println(encryption.encrypt(apiKey));
    }
}
