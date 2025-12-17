package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class BitSetIndexEntrySerializerTest {

    private final BitSetIndexEntrySerializer serDe = new BitSetIndexEntrySerializer();

    @Test
    public void testSerDeBytes() {
        BitSetIndexEntry entry = createEntry();

        byte[] data = serDe.serialize(entry);
        BitSetIndexEntry result = serDe.deserialize(data);

        assertMap(entry, result);
    }

    @Test
    public void testSerDeString() {
        BitSetIndexEntry entry = createEntry();

        String s = serDe.serializeToString(entry);
        BitSetIndexEntry result = serDe.deserializeFromString(s);

        assertMap(entry, result);
    }

    private BitSetIndexEntry createEntry() {
        String day = "20220101";
        Map<String,BitSet> shards = new HashMap<>();
        shards.put("FIELD_A", BitSetFactory.create(0, 2, 4, 6, 8));
        shards.put("FIELD_B", BitSetFactory.create(1, 3, 5, 7, 9));
        return new BitSetIndexEntry(day, shards);
    }

    private void assertMap(BitSetIndexEntry expected, BitSetIndexEntry result) {
        assertEquals(expected.getYearOrDay(), result.getYearOrDay());
        assertEquals(expected.getEntries(), result.getEntries());
        assertEquals(expected.getEntries().keySet(), result.getEntries().keySet());
        for (String key : expected.getEntries().keySet()) {
            assertEquals(expected.getEntries().get(key), result.getEntries().get(key));
        }
    }
}
