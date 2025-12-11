package datawave.ingest.table.aggregator.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.BitSet;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

public class ShardedDayIndexKeyParserTest implements IndexKeyConversionTests {

    private final ShardedDayIndexKeyParser parser = new ShardedDayIndexKeyParser();

    @Test
    public void testStandardShardIndexKey() {
        Key key = new Key("value", "FIELD", "20250606_123\u0000datatype-a", "VIZ-A");
        parser.parse(key);
        assertEquals("value", parser.getValue());
        assertEquals("FIELD", parser.getField());
        assertEquals("20250606_123", parser.getDateAndShard());
        assertEquals("20250606", parser.getDate());
        assertEquals("datatype-a", parser.getDatatype());

        Key expected = new Key("20250606\u0000value", "FIELD", "datatype-a", "VIZ-A");
        assertEquals(expected, parser.convert());

        BitSet expectedBits = new BitSet();
        expectedBits.set(123);
        assertEquals(expectedBits, parser.getBitset());
    }

    @Test
    public void testTruncatedShardIndexKey() {
        Key key = new Key("value", "FIELD", "20250606\u0000datatype-a", "VIZ-A");
        parser.parse(key);
        assertEquals("value", parser.getValue());
        assertEquals("FIELD", parser.getField());
        assertEquals("20250606", parser.getDateAndShard());
        assertEquals("20250606", parser.getDate());
        assertEquals("datatype-a", parser.getDatatype());

        // parser will convert a truncated index key, but the bitset is a pass-through
        Key expected = new Key("20250606\u0000value", "FIELD", "datatype-a", "VIZ-A");
        assertEquals(expected, parser.convert());
        assertNull(parser.getBitset());
    }

    @Test
    public void testShardedDayIndexKey() {
        Key key = new Key("20250606\u0000value", "FIELD", "datatype-a", "VIZ-A");
        parser.parse(key);
        assertEquals("value", parser.getValue());
        assertEquals("FIELD", parser.getField());
        assertEquals("20250606", parser.getDateAndShard());
        assertEquals("20250606", parser.getDate());
        assertEquals("datatype-a", parser.getDatatype());

        // parser should be a pass-through. convert returns the original key, getBitSet returns null
        assertEquals(key, parser.convert());
        assertNull(parser.getBitset());
    }

    @Test
    public void testShardedYearIndexKey() {
        Key key = new Key("2025\u0000value", "FIELD", "datatype-a", "VIZ-A");
        parser.parse(key);
        assertEquals("value", parser.getValue());
        assertEquals("FIELD", parser.getField());
        assertEquals("2025", parser.getDateAndShard());
        assertEquals("2025", parser.getDate());
        assertEquals("datatype-a", parser.getDatatype());

        // parser should be a pass-through. convert returns the original key, getBitSet returns null
        assertEquals(key, parser.convert());
        assertNull(parser.getBitset());
    }

}
