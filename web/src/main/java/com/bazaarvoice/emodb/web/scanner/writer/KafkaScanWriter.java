package com.bazaarvoice.emodb.web.scanner.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaScanWriter implements ScanWriter {

    private final Producer<String, JsonNode> _producer;
    private final ObjectMapper _mapper;
    private final Clock _clock;
    private final String _topicName;
    private final Map<TransferKey, KafkaScanDestinationWriter> _writers;

    @Inject
    public KafkaScanWriter(Producer<String, JsonNode> producer, ObjectMapper objectMapper, Clock clock,
                           @Assisted URI baseUri) {
        _producer = checkNotNull(producer, "kakfaProducer");
        _mapper = checkNotNull(objectMapper, "objectMapper");
        _clock = checkNotNull(clock, "clock");
        _topicName  = checkNotNull(baseUri.getHost(), "topicName");
        checkArgument(!_topicName.isEmpty());
        _writers = new HashMap<>();
    }

    @Override
    public synchronized ScanDestinationWriter writeShardRows(String tableName, String placement, int shardId, long tableUuid) throws IOException, InterruptedException {
        KafkaScanDestinationWriter kafkaScanDestinationWriter = new KafkaScanDestinationWriter(_producer, _mapper, _topicName);
        _writers.put(new TransferKey(tableUuid, shardId), kafkaScanDestinationWriter);
        return kafkaScanDestinationWriter;
    }

    @Override
    public WaitForAllTransfersCompleteResult waitForAllTransfersComplete(Duration duration) throws IOException, InterruptedException {
        Instant startTime = _clock.instant();
        Map<TransferKey, TransferStatus> statusMap = new HashMap<>();
        while (startTime.plus(duration).isAfter(_clock.instant())) {
            for (Map.Entry<TransferKey, KafkaScanDestinationWriter> entry : _writers.entrySet()) {
                if (!entry.getValue().isFinishedUploading()) {
                    statusMap.put(entry.getKey(), entry.getValue().getTransferStatus(entry.getKey()));
                }
            }
            if (statusMap.isEmpty()) {
                break;
            }
            Thread.sleep(Duration.ofSeconds(1).toMillis());
        }

        return new WaitForAllTransfersCompleteResult(statusMap);
    }

    @Override
    public boolean writeScanComplete(String scanId, Date startTime) throws IOException {
        // This is a No-op in this implementation
        return true;
    }

    @Override
    public void close() throws IOException {
        // NO-OP
    }
}
