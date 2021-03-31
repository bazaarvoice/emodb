package com.bazaarvoice.gatekeeper.emodb.commons;

import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IModuleFactory;
import org.testng.ITestContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class TestModuleFactory implements IModuleFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestModuleFactory.class);

    private static Module testModule;

    private static final List<String> PARAMS = ImmutableList.of(
            "apiKey",
            "zkNamespace",
            "zkConnection",
            "clusterName",
            "placement",
            "remotePlacement",
            "mediaPlacement",
            "clientHttpTimeout",
            "clientHttpKeepAlive"
    );

    @Override
    public Module createModule(ITestContext iTestContext, Class<?> aClass) {
        if (testModule == null) {
            Properties systemProperties = System.getProperties();

            Map<String, String> params = PARAMS.stream()
                    .filter(systemProperties::containsKey)
                    .collect(Collectors.toMap(s -> s, Objects.requireNonNull(System::getProperty)));

            testModule = new BaseTestsModule(params);
        }
        LOGGER.info("Using emo version: {}({})", System.getProperties().getProperty("emoVersion"),
                DataStoreClient.class.getPackage().getImplementationVersion());
        return testModule;
    }
}
