package datawave.query.predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;

public class EventDataQueryFieldFilterTest {

    @Test
    public void testFieldAccept() {
        EventDataQueryFieldFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A"));

        assertTrue(filter.apply(createEntry("FIELD_A")));
        assertTrue(filter.apply(createEntry("FIELD_A")));
        assertTrue(filter.apply(createEntry("FIELD_A")));
    }

    @Test
    public void testFieldRejection_sameFieldRepeated() {
        EventDataQueryFieldFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A"));

        Map.Entry<Key,String> entry = createEntry("FIELD_B");
        assertFalse(filter.apply(entry));
        assertFalse(filter.apply(entry));
        assertFalse(filter.apply(entry));
        assertNull(filter.getSeekRange(entry.getKey(), null, false));
    }

    @Test
    public void testFieldRejection_differentFieldRepeated() {
        EventDataQueryFieldFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A"));

        Map.Entry<Key,String> entry = createEntry("FIELD_B");
        assertFalse(filter.apply(createEntry("FIELD_B")));
        assertFalse(filter.apply(createEntry("FIELD_C")));
        assertFalse(filter.apply(createEntry("FIELD_D")));
        assertNull(filter.getSeekRange(entry.getKey(), null, false));
    }

    // tests the case where no key is accepted and the filter seeks to the first field
    @Test
    public void testGetSeekRange_seekForwardToFirstField() {
        EventDataQueryFieldFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_B")).withMaxNextCount(1);

        Map.Entry<Key,String> entry = createEntry("FIELD_A");
        assertFalse(filter.apply(entry));
        assertFalse(filter.apply(entry));
        assertFalse(filter.apply(entry));

        Range range = filter.getSeekRange(entry.getKey(), null, false);
        assertSeekRangeStartKey(range, new Key("row", "datatype\0uid", "FIELD_B\0"));
    }

    // tests the case where some keys were accepted and the filter seeks to the second field
    @Test
    public void testGetSeekRange_seekToSecondField() {
        EventDataQueryFieldFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_B", "FIELD_D")).withMaxNextCount(1);

        Map.Entry<Key,String> entry = createEntry("FIELD_C");
        assertFalse(filter.apply(entry));
        assertFalse(filter.apply(entry));
        assertFalse(filter.apply(entry));

        Range range = filter.getSeekRange(entry.getKey(), null, false);
        assertSeekRangeStartKey(range, new Key("row", "datatype\0uid", "FIELD_D\0"));
    }

    @Test
    public void testGetSeekRange_rolloverRange() {
        EventDataQueryFieldFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD_A", "FIELD_B")).withMaxNextCount(1);

        Map.Entry<Key,String> entry = createEntry("FIELD_C");
        assertFalse(filter.apply(entry));
        assertFalse(filter.apply(entry));
        assertFalse(filter.apply(entry));

        Range range = filter.getSeekRange(entry.getKey(), null, false);
        assertSeekRangeStartKey(range, new Key("row", "datatype\0uid\0"));
    }

    private Map.Entry<Key,String> createEntry(String field) {
        Key key = new Key("row", "datatype\u0000uid", field + "\u0000value");
        return new AbstractMap.SimpleEntry<>(key, "");
    }

    private void assertSeekRangeStartKey(Range range, Key expected) {
        assertEquals(expected, range.getStartKey());
    }

}
