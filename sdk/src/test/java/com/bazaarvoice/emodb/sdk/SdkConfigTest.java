package com.bazaarvoice.emodb.sdk;

import com.bazaarvoice.emodb.web.EmoConfiguration;
import com.bazaarvoice.emodb.web.util.EmoServiceObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import org.testng.annotations.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

public class SdkConfigTest {
    @Test
    public void ensureSdkDefaultConfigDeserialization()
            throws IOException, URISyntaxException, ConfigurationException {
        // This test makes sure that we haven't forgotten to update the emodb sdk default config file when we add/remove properties
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        ObjectMapper mapper = EmoServiceObjectMapperFactory.build(new YAMLFactory());
        ConfigurationFactory configurationFactory = new ConfigurationFactory(EmoConfiguration.class, validator, mapper, "dw");
        // Make sure that our config files are up to date
        configurationFactory.build(
        new File(EmoStartMojo.class.getResource("/emodb-default-config.yaml").toURI()));
    }
}
