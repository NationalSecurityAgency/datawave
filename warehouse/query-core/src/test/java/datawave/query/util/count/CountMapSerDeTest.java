package datawave.query.util.count;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Test for {@link CountMapSerDe}
 */
class CountMapSerDeTest {
    private final CountMapSerDe serDe = new CountMapSerDe();

    @Test
    void testSerDeBytes() {
        CountMap map = new CountMap();
        map.put("FIELD_A", 12L);
        map.put("FIELD_B", 23L);

        byte[] data = serDe.serialize(map);
        assertEquals(18, data.length); // 35 without optimizing integers/longs
        CountMap deserialized = serDe.deserialize(data);

        assertMap(map, deserialized);
    }

    @Test
    void testSerDeString() {
        CountMap map = new CountMap();
        map.put("FIELD_A", 12L);
        map.put("FIELD_B", 23L);

        String s = serDe.serializeToString(map);
        assertEquals(18, s.length()); // 35 without optimizing integers/longs
        CountMap deserialized = serDe.deserializeFromString(s);

        assertMap(map, deserialized);
    }

    private void assertMap(CountMap expected, CountMap map) {
        assertEquals(expected.keySet(), map.keySet());
        for (String key : expected.keySet()) {
            assertEquals(expected.get(key), map.get(key));
        }
        assertEquals(expected, map);
    }

}
