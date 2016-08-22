package com.bazaarvoice.emodb.sor.api;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class CoordinateTest {

    @Test
    public void testCoordinate() {
        Coordinate coord = Coordinate.of("my-table", "my-id");
        assertEquals(coord.getTable(), "my-table");
        assertEquals(coord.getId(), "my-id");
        assertEquals(coord.toString(), "my-table/my-id");
        assertEquals(coord.asJson(), ImmutableMap.of(Intrinsic.TABLE, "my-table", Intrinsic.ID, "my-id"));
        assertEquals(coord, Coordinate.of("my-table", "my-id"));
        assertNotEquals(coord, Coordinate.of("my-table", "my-id2"));
        assertNotEquals(coord, Coordinate.of("my-table2", "my-id"));
        assertEquals(coord.hashCode(), Coordinate.of("my-table", "my-id").hashCode());
        assertEquals(Coordinate.parse(coord.toString()), coord);
        assertEquals(Coordinate.fromJson(coord.asJson()), coord);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseInvalidString() {
        Coordinate.parse("abc");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseMissingTable() {
        Coordinate.parse("/id");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseInvalidTable() {
        Coordinate.parse("Review/id");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseMissingKey() {
        Coordinate.parse("review/");
    }

    @Test
    public void testSpecialCharacterKey() {
        String id = "/ \t\n\u0100'\"";
        assertEquals(Coordinate.parse("review/" + id), Coordinate.of("review", id));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testJsonMissingTable() {
        Coordinate.fromJson(ImmutableMap.of(Intrinsic.ID, "my-id"));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testJsonMissingId() {
        Coordinate.fromJson(ImmutableMap.of(Intrinsic.TABLE, "my-table"));
    }
}
