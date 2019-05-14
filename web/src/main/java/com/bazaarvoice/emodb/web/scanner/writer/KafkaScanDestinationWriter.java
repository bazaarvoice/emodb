package com.bazaarvoice.emodb.web.scanner.writer;

import com.bazaarvoice.emodb.sor.api.Coordinate;
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

public class KafkaScanDestinationWriter implements ScanDestinationWriter {

    private final Producer<String, JsonNode> _producer;
    private final ObjectMapper _mapper;
    private final String _topic;
    private final BlockingQueue<Future<RecordMetadata>> _futureQueue;
    private final ExecutorService _futureGettingService;
    private final AtomicReference<Throwable> _error = new AtomicReference<>();
    private boolean _closed;
    private int _bytesTransferred;
    private int _bytesAdded;

    public KafkaScanDestinationWriter(Producer<String, JsonNode> producer, ObjectMapper objectMapper, String topic) {
        _producer = producer;
        _mapper = objectMapper;
        _topic = topic;
        _futureQueue = new ArrayBlockingQueue<>(10000);
        _futureGettingService = Executors.newSingleThreadExecutor();
        _closed = false;
        _bytesTransferred = 0;
        _bytesAdded = 0;


        _futureGettingService.submit(this::collectFuture);

    }

    private void collectFuture() {
        while (true) {
            try {
                Future<RecordMetadata> future;
                if ((future = _futureQueue.poll(100, TimeUnit.MILLISECONDS)) != null) {
                    RecordMetadata recordMetadata = future.get();
                    _bytesTransferred += recordMetadata.serializedKeySize() + recordMetadata.serializedValueSize();
                } else if (_futureGettingService.isShutdown()) {
                    break;
                }
            } catch (Exception e) {
                _error.compareAndSet(null, e);
                // TODO: log exception
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
        _futureQueue.put(_producer.send(record));

    }



    @Override
    public void closeAndCancel() {
        _closed = true;
        _futureGettingService.shutdownNow();
        _error.compareAndSet(null, new RuntimeException("Kafka upload canceled."));
    }

    @Override
    public void closeAndTransferAsync(Optional<Integer> finalPartCount) throws IOException {
        _closed = true;
        _producer.flush();
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


}
