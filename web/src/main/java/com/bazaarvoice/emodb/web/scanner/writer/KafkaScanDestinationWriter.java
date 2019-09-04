package com.bazaarvoice.emodb.web.scanner.writer;

import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaScanDestinationWriter implements ScanDestinationWriter {

    private static Logger _log = LoggerFactory.getLogger(KafkaScanDestinationWriter.class);

    private final Producer<String, JsonNode> _producer;
    private final ObjectMapper _mapper;
    private final String _topic;
    private final BlockingQueue<CoordinateAndFuture> _futureQueue;
    private final ExecutorService _futureGettingService;
    private final AtomicReference<Throwable> _error = new AtomicReference<>();
    private volatile boolean _closed;
    private int _bytesTransferred;
    private int _bytesAdded;

    private final Meter _blockingQueueFullMeter;


    public KafkaScanDestinationWriter(Producer<String, JsonNode> producer, ObjectMapper objectMapper, String topic,
                                      MetricRegistry metricRegistry) {
        _producer = producer;
        _mapper = objectMapper;
        _topic = topic;
        _futureQueue = new ArrayBlockingQueue<>(10000);
        _futureGettingService = Executors.newSingleThreadExecutor();
        _closed = false;
        _bytesTransferred = 0;
        _bytesAdded = 0;

        _blockingQueueFullMeter = metricRegistry.meter(getMetricName("blockingQueueFull"));

        _futureGettingService.submit(this::collectFuture);

    }


    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.scanner", "KafkaScanWriter", name);
    }

    private void collectFuture() {
        while (true) {
            Coordinate coordinate = null;
            try {
                CoordinateAndFuture coordinateAndFuture;
                if ((coordinateAndFuture = _futureQueue.poll(100, TimeUnit.MILLISECONDS)) != null) {
                    coordinate = coordinateAndFuture.getCoordinate();
                    RecordMetadata recordMetadata = coordinateAndFuture.getFuture().get();
                    _bytesTransferred += recordMetadata.serializedKeySize() + recordMetadata.serializedValueSize();
                } else if (_futureGettingService.isShutdown() && _futureQueue.isEmpty()) {
                    break;
                }
            } catch (Exception e) {
                _error.compareAndSet(null, e);
                _log.error("Error sending coordinate {} to Kafka: ", coordinate, e);
            }
        }
    }

    @Override
    public void writeDocument(Map<String, Object> document) throws IOException, InterruptedException {
        if (_error.get() != null) {
            throw new IOException("Message failed to send to kafka", _error.get());
        }

        if (_closed) {
            throw new RuntimeException("This writer has already been closed");
        }
        ProducerRecord<String, JsonNode> record = new ProducerRecord<>(_topic, Coordinate.fromJson(document).toString(), _mapper.valueToTree(document));
        _bytesAdded += record.key().length() + record.value().size();
        _futureQueue.put(new CoordinateAndFuture(Coordinate.fromJson(document), _producer.send(record)));

        // flush the producer if we are out of space in the producer
        if (_futureQueue.remainingCapacity() == 0) {
            _blockingQueueFullMeter.mark();
            _producer.flush();
        }

    }



    @Override
    public void closeAndCancel() {
        _closed = true;
        _error.compareAndSet(null, new RuntimeException("Kafka upload canceled."));
        _futureGettingService.shutdownNow();
    }

    @Override
    public void closeAndTransferAsync(Optional<Integer> finalPartCount) throws IOException {
        _closed = true;
        _futureGettingService.shutdown();
    }

    public boolean isFinishedUploading() throws IOException {
        boolean isFinishedUploading = _closed && _futureGettingService.isTerminated();

        if (_error.get() != null) {
            throw new IOException("Failed to finish uploading to kafka", _error.get());
        }

        return isFinishedUploading;
    }

    public TransferStatus getTransferStatus(TransferKey transferKey) {
        return new TransferStatus(transferKey, _bytesAdded, 1, _bytesTransferred);
    }


    private static class CoordinateAndFuture {
        private final Coordinate _coordinate;
        private final Future<RecordMetadata> _future;

        public CoordinateAndFuture(Coordinate coordinate, Future<RecordMetadata> future) {
            _coordinate = coordinate;
            _future = future;
        }

        public Coordinate getCoordinate() {
            return _coordinate;
        }

        public Future<RecordMetadata> getFuture() {
            return _future;
        }
    }

}
