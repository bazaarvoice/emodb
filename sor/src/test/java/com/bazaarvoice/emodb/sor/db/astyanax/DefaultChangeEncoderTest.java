package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.serializers.StringSerializer;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertEquals;

public class DefaultChangeEncoderTest {

    @Test
    public void testD2Decoding() {
        String noTags = "D2:[]:{..,\"name\":\"bob\"}";
        String oneTag = "D2:[\"one\"]:{..,\"name\":\"bob\"}";
        String tags = "D2:[\"one\",\"two\",\"three\"]:{..,\"name\":\"bob\"}";

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
    public void testEncodeDecodeD2() {
        Delta delta = Deltas.mapBuilder().put("name", "bob").remove("x").build();
        Set<String> tags = ImmutableSet.of("tag0","tag1");
        ChangeEncoder changeEncoder = new DefaultChangeEncoder();
        // Encode and then decode the said delta, and verify if Change is as expected
        Change change = changeEncoder.decodeChange(TimeUUIDs.newUUID(),
                StringSerializer.get().fromString(changeEncoder.encodeDelta(delta.toString(), tags)));
        assertEquals(change.getDelta(), delta);
        assertEquals(change.getTags(), tags);
    }

    private void verifyDecodedChange(String encodedDelta, Delta expectedDelta, ImmutableSet<String> tags) {
        ChangeEncoder changeEncoder = new DefaultChangeEncoder();
        Change change = changeEncoder.decodeChange(TimeUUIDs.newUUID(), StringSerializer.get().toByteBuffer(encodedDelta));
        assertEquals(change.getDelta(), expectedDelta);
        assertEquals(change.getTags(), tags);
    }



}
