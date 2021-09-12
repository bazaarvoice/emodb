package com.bazaarvoice.emodb.kafka;

import com.bazaarvoice.emodb.common.json.CustomJsonObjectMapperFactory;
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

public class KafkaConfigTest {
    @Test
    public void ensureKafkaDefaultConfigDeserialization()
        throws IOException, URISyntaxException, ConfigurationException {
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        ObjectMapper mapper = CustomJsonObjectMapperFactory.build(new YAMLFactory());
        ConfigurationFactory configurationFactory = new ConfigurationFactory(KafkaConfiguration.class, validator, mapper, "dw");
        // Make sure that our config files are up to date
        configurationFactory.build(
            new File(KafkaConfiguration.class.getResource("/emodb-kafka-config.yaml").toURI()));
    }
}
