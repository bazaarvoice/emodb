package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

public class MegabusSourceTest {
    private static final Producer<String, JsonNode> producer1 = mock(Producer.class);
    private static final Producer<String, JsonNode> producer2 = mock(Producer.class);
    private static final Topic topic = new Topic("mytopic", 16, (short) 1);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Clock clock = mock(Clock.class);

    @Test
    public void testTouchOneCoordinate() {
        final String table = "table";
        final String documentId = "id";

        Coordinate coordinate = Coordinate.of(table, documentId);
        DefaultMegabusSource megabusSource = new DefaultMegabusSource(producer1, topic, objectMapper, clock);
        megabusSource.asyncSendFutures(Collections.singletonList(coordinate));

        ArgumentCaptor<ProducerRecord> argument = ArgumentCaptor.forClass(ProducerRecord.class);

        // verify send is called ONE time with the right values.
        verify(producer1, times(1)).send(argument.capture());
        assertEquals(argument.getValue().value(), objectMapper.valueToTree(Collections.singletonList((new MegabusRef(table, documentId, TimeUUIDs.minimumUuid(), clock.instant(), MegabusRef.RefType.TOUCH)))));
        assertEquals(argument.getValue().topic(), topic.getName());
    }

    @Test
    public void testTouchTwoCoordinates() {
        final String table1 = "table1";
        final String documentId1 = "id1";
        final String table2 = "table2";
        final String documentId2 = "id2";

        List<Coordinate> coordinates = Stream.of(Coordinate.of(table1, documentId1), Coordinate.of(table2, documentId2)).collect(Collectors.toList());
        DefaultMegabusSource megabusSource = new DefaultMegabusSource(producer2, topic, objectMapper, clock);
        megabusSource.asyncSendFutures(coordinates);

        ArgumentCaptor<ProducerRecord> argument = ArgumentCaptor.forClass(ProducerRecord.class);

        // verify send is called TWO times for the 2 Coordinates with right values.
        verify(producer2, times(2)).send(argument.capture());

        List<ProducerRecord> allRecords = argument.getAllValues();

        assertEquals(allRecords.get(0).value(), objectMapper.valueToTree(Collections.singletonList((new MegabusRef(table1, documentId1, TimeUUIDs.minimumUuid(), clock.instant(), MegabusRef.RefType.TOUCH)))));
        assertEquals(allRecords.get(0).topic(), topic.getName());

        assertEquals(allRecords.get(1).value(), objectMapper.valueToTree(Collections.singletonList((new MegabusRef(table2, documentId2, TimeUUIDs.minimumUuid(), clock.instant(), MegabusRef.RefType.TOUCH)))));
        assertEquals(allRecords.get(1).topic(), topic.getName());
    }
}
