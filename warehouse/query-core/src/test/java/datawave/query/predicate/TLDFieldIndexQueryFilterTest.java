package datawave.query.predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class TLDFieldIndexQueryFilterTest {

    private final Key tldField1 = new Key("row", "fi\0FIELD1", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key tldField2 = new Key("row", "fi\0FIELD2", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key tldField3 = new Key("row", "fi\0FIELD3", "value\0datatype\0d8zay2.-3pnndm.-anolok");

    private final Key childField1 = new Key("row", "fi\0FIELD1", "value\0datatype\0d8zay2.-3pnndm.-anolok.23");
    private final Key childField2 = new Key("row", "fi\0FIELD2", "value\0datatype\0d8zay2.-3pnndm.-anolok.33");
    private final Key childField3 = new Key("row", "fi\0FIELD3", "value\0datatype\0d8zay2.-3pnndm.-anolok.45");

    @Test
    public void testTLDFieldIndexQueryFilter() {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELD1", "FIELD2");
        TLDFieldIndexQueryFilter filter = new TLDFieldIndexQueryFilter(indexOnlyFields);

        // retain query index-only fields in the tld
        assertTrue(filter.keep(tldField1));
        assertTrue(filter.keep(tldField2));
        assertFalse(filter.keep(tldField3));

        // retain ALL non-tld fields
        assertTrue(filter.keep(childField1));
        assertTrue(filter.keep(childField2));
        assertTrue(filter.keep(childField3));
    }

    @Test
    public void testClone() {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELD1", "FIELD2");
        EventDataQueryFilter filter = new TLDFieldIndexQueryFilter(indexOnlyFields);
        EventDataQueryFilter clone = filter.clone();
        assertNotEquals(filter, clone);
    }

}
