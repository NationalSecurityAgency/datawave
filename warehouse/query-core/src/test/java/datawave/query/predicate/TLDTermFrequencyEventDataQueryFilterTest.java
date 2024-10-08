package datawave.query.predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TLDTermFrequencyEventDataQueryFilterTest {

    private final Key tldField1 = new Key("row", "fi\0FIELD1", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key tldField2 = new Key("row", "fi\0FIELD2", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key tldField3 = new Key("row", "fi\0FIELD3", "value\0datatype\0d8zay2.-3pnndm.-anolok");

    private final Key childField1 = new Key("row", "fi\0FIELD1", "value\0datatype\0d8zay2.-3pnndm.-anolok.23");
    private final Key childField2 = new Key("row", "fi\0FIELD2", "value\0datatype\0d8zay2.-3pnndm.-anolok.33");
    private final Key childField3 = new Key("row", "fi\0FIELD3", "value\0datatype\0d8zay2.-3pnndm.-anolok.45");

    @Test
    public void testTLDTermFrequencyEventDataQueryFilter() {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELD1", "FIELD2");
        TLDTermFrequencyEventDataQueryFilter filter = new TLDTermFrequencyEventDataQueryFilter(indexOnlyFields, Set.of("FIELD1"));

        // retain query index-only fields in the tld
        assertTrue(filter.keep(tldField1));
        assertFalse(filter.keep(tldField2));
        assertFalse(filter.keep(tldField3));

        // retain ALL non-tld fields
        assertTrue(filter.keep(childField1));
        assertFalse(filter.keep(childField2));
        assertFalse(filter.keep(childField3));
    }

}
