package datawave.query.predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    // grouped/content-context-hashed instances of the same fields, e.g. as written for a tokenized content field with
    // content-context hashing enabled. The hash notation is only ever present on the term-frequency field name itself;
    // indexOnlyFields/fields are always configured with base (un-grouped) field names by TLDIndexBuildingVisitor.
    private final Key tldField1Grouped = new Key("row", "fi\0FIELD1.hash1", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key tldField3Grouped = new Key("row", "fi\0FIELD3.hash1", "value\0datatype\0d8zay2.-3pnndm.-anolok");

    private final Key childField1Grouped = new Key("row", "fi\0FIELD1.hash2", "value\0datatype\0d8zay2.-3pnndm.-anolok.23");
    private final Key childField3Grouped = new Key("row", "fi\0FIELD3.hash2", "value\0datatype\0d8zay2.-3pnndm.-anolok.45");

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

    /**
     * A grouped/content-context instance of a requested field (e.g. {@code FIELD1.hash1}) must be recognized as satisfying a request for its base field
     * ({@code FIELD1}), the same as an un-grouped instance would be -- both for a root (tld) key, where {@code keep()} additionally requires the field to be
     * found in {@code indexOnlyFields}, and for a child key.
     */
    @Test
    public void testTLDTermFrequencyEventDataQueryFilterWithGroupedFieldNames() {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELD1", "FIELD2");
        TLDTermFrequencyEventDataQueryFilter filter = new TLDTermFrequencyEventDataQueryFilter(indexOnlyFields, Set.of("FIELD1"));

        // a grouped instance of the requested, index-only root field is kept, exactly as the un-grouped instance is
        assertTrue(filter.keep(tldField1Grouped));
        // a grouped instance of a field that was not requested is still not kept
        assertFalse(filter.keep(tldField3Grouped));

        // a grouped instance of the requested field is kept on a child key too
        assertTrue(filter.keep(childField1Grouped));
        // a grouped instance of a field that was not requested is still not kept on a child key
        assertFalse(filter.keep(childField3Grouped));
    }

}
