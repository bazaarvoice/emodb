package com.bazaarvoice.megabus.service;

import com.bazaarvoice.emodb.kafka.metrics.DropwizardMetricsReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.base.Charsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class ProducerExceptionHandler implements ProductionExceptionHandler {

    private static final Logger _log = LoggerFactory.getLogger(ProducerExceptionHandler.class);
    private static final Counter _producerExceptionCounter = SharedMetricRegistries
        .getOrCreate(DropwizardMetricsReporter.REGISTRY_NAME)
        .counter("com.emodb.megabus.kafka-producer-exception.count");

    @Override public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record, final Exception exception) {
        _log.error("exception writing record with key[{}]:", new String(record.key(), Charsets.UTF_8), exception);
        _producerExceptionCounter.inc();
        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override public void configure(final Map<String, ?> map) {

    }
}
