package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.json.deferred.LazyJsonMap;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.delta.Delete;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.serializers.StringSerializer;
import org.testng.annotations.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DefaultChangeEncoderTest {

    @Test
    public void testD3Decoding() {
        String noTags = "D3:[]:0:{..,\"name\":\"bob\"}";
        String oneTag = "D3:[\"one\"]:0:{..,\"name\":\"bob\"}";
        String tags = "D3:[\"one\",\"two\",\"three\"]:0:{..,\"name\":\"bob\"}";

        Delta expectedDelta = Deltas.fromString("{..,\"name\":\"bob\"}");


        verifyDecodedChange(noTags, expectedDelta, ImmutableSet.<String>of());
        verifyDecodedChange(oneTag, expectedDelta, ImmutableSet.of("one"));
        verifyDecodedChange(tags, expectedDelta, ImmutableSet.of("one", "two", "three"));
    }

    @Test
    public void testLegacyD1Decoding() {
        String legacyD1 = "D1:{..,\"name\":\"bob\"}";
        Delta expectedDelta = Deltas.fromString("{..,\"name\":\"bob\"}");
        verifyDecodedChange(legacyD1, expectedDelta, ImmutableSet.<String>of());
    }

    @Test
    public void testLegacyD2Decoding() {
        String legacyD2 = "D2:[\"tag0\",\"tag1\"]:{..,\"name\":\"bob\"}";
        Delta expectedDelta = Deltas.fromString("{..,\"name\":\"bob\"}");
        verifyDecodedChange(legacyD2, expectedDelta, ImmutableSet.of("tag0", "tag1"));
    }

    @Test
    public void testEncodeDecodeD2() {
        Delta delta = Deltas.mapBuilder().put("name", "bob").remove("x").build();
        Set<String> tags = ImmutableSet.of("tag0","tag1");
        ChangeEncoder changeEncoder = new DefaultChangeEncoder(2);
        // Encode and then decode the said delta, and verify if Change is as expected
        String encodedDelta = changeEncoder.encodeDelta(delta.toString(), EnumSet.of(ChangeFlag.MAP_DELTA), tags, new StringBuilder());
        assertEquals(encodedDelta, "D2:[\"tag0\",\"tag1\"]:{..,\"name\":\"bob\",\"x\":~}");
        Change change = changeEncoder.decodeChange(TimeUUIDs.newUUID(), StringSerializer.get().fromString(encodedDelta));
        assertEquals(change.getDelta(), delta);
        assertEquals(change.getTags(), tags);
    }

    @Test
    public void testEncodeDecodeD3() {
        Delta delta = Deltas.mapBuilder().put("name", "bob").remove("x").build();
        Set<String> tags = ImmutableSet.of("tag0","tag1");
        ChangeEncoder changeEncoder = new DefaultChangeEncoder(3);
        // Encode and then decode the said delta, and verify if Change is as expected
        String encodedDelta = changeEncoder.encodeDelta(delta.toString(), EnumSet.of(ChangeFlag.MAP_DELTA), tags, new StringBuilder());
        assertEquals(encodedDelta, "D3:[\"tag0\",\"tag1\"]:M:{..,\"name\":\"bob\",\"x\":~}");
        Change change = changeEncoder.decodeChange(TimeUUIDs.newUUID(), StringSerializer.get().fromString(encodedDelta));
        // Because the change contains a lazy delta it will not be the exact same instance as "delta"
        assertEquals(change.getDelta().toString(), delta.toString());
        assertFalse(change.getDelta().isConstant());
        assertEquals(change.getTags(), tags);
    }

    @Test
    public void testDecodeCompactionWithMapLiteral() {
        String c1 = "C1:{\"count\":4,\"first\":\"6b6dff41-e50b-11e5-b18e-0e83e95d75a9\",\"cutoff\":\"741bb5bc-a5dc-11e6-8d58-123665dcce6e\"," +
                "\"cutoffSignature\":\"b6fe61d13972264e9d7ab0c230c82855\",\"lastContentMutation\":\"cef920a5-fbd4-11e5-b18e-0e83e95d75a9\"," +
                "\"lastMutation\":\"cef920a5-fbd4-11e5-b18e-0e83e95d75a9\",\"compactedDelta\":\"{\\\"active\\\":true}\",\"lastTags\":[\"tag1\"]}";
        ChangeEncoder changeEncoder = new DefaultChangeEncoder();
        Compaction compaction = changeEncoder.decodeCompaction(StringSerializer.get().fromString(c1));
        assertEquals(compaction.getCount(), 4);
        assertEquals(compaction.getLastTags(), ImmutableSet.of("tag1"));
        // Compacted delta should be a lazy map literal
        assertTrue(compaction.getCompactedDelta().isConstant());
        assertTrue(compaction.getCompactedDelta() instanceof Literal);
        assertTrue(((Literal) compaction.getCompactedDelta()).getValue() instanceof LazyJsonMap);
        assertEquals(compaction.getCompactedDelta(), Deltas.literal(ImmutableMap.of("active", true)));
    }

    @Test
    public void testDecodeCompactionWithDeletionDelta() {
        String c1 = "C1:{\"count\":4,\"first\":\"6b6dff41-e50b-11e5-b18e-0e83e95d75a9\",\"cutoff\":\"741bb5bc-a5dc-11e6-8d58-123665dcce6e\"," +
                "\"cutoffSignature\":\"b6fe61d13972264e9d7ab0c230c82855\",\"lastContentMutation\":\"cef920a5-fbd4-11e5-b18e-0e83e95d75a9\"," +
                "\"lastMutation\":\"cef920a5-fbd4-11e5-b18e-0e83e95d75a9\",\"compactedDelta\":\"~\",\"lastTags\":[\"tag1\"]}";
        ChangeEncoder changeEncoder = new DefaultChangeEncoder();
        Compaction compaction = changeEncoder.decodeCompaction(StringSerializer.get().fromString(c1));
        assertEquals(compaction.getCount(), 4);
        assertEquals(compaction.getLastTags(), ImmutableSet.of("tag1"));
        // Compacted delta should be a delete delta
        assertTrue(compaction.getCompactedDelta().isConstant());
        assertTrue(compaction.getCompactedDelta() instanceof Delete);
        assertEquals(compaction.getCompactedDelta(), Deltas.delete());
    }

    private void verifyDecodedChange(String encodedDelta, Delta expectedDelta, ImmutableSet<String> tags) {
        ChangeEncoder changeEncoder = new DefaultChangeEncoder();
        Change change = changeEncoder.decodeChange(TimeUUIDs.newUUID(), StringSerializer.get().toByteBuffer(encodedDelta));
        assertEquals(change.getDelta().toString(), expectedDelta.toString());
        assertEquals(change.getTags(), tags);
    }
}