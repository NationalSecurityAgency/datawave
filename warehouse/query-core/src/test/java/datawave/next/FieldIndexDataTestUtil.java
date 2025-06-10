package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;

import com.google.common.base.Preconditions;

import datawave.next.stats.DocumentIteratorStats;
import datawave.query.jexl.JexlASTHelper;

/**
 * Test utility with common methods for writing data asserting results.
 * <p>
 * Extending classes can easily write singleton or bulk test data, create and drive an iterator and assert results.
 */
public abstract class FieldIndexDataTestUtil {

    protected final String row = "20250101_0";
    protected final Value EMPTY_VALUE = new Value();

    private static final String DEFAULT_DATATYPE = "datatype-a";

    protected final TreeMap<Key,Value> data = new TreeMap<>();
    protected final SortedSet<String> datatypes = new TreeSet<>();
    protected LongRange timeFilter = null;
    protected final SortedSet<Key> results = new TreeSet<>();

    protected Key min;
    protected Key max;

    protected String query;
    protected DocumentIteratorStats stats;

    protected void clearState() {
        data.clear();
        datatypes.clear();
        timeFilter = null;
        results.clear();
        stats = null;
        min = null;
        max = null;
    }

    protected void withQuery(String query) {
        this.query = query;
    }

    public void withDataTypes(String... datatypes) {
        this.datatypes.clear();
        this.datatypes.addAll(Arrays.asList(datatypes));
    }

    public void withTimeFilter(LongRange timeFilter) {
        this.timeFilter = timeFilter;
    }

    /**
     * Shim for ease of testing.
     *
     * @param min
     *            the minimum datatype\0uid
     * @param max
     *            the maximum datatype\0uid
     */
    public void withMinMax(String min, String max) {
        this.min = new Key(row, min);
        this.max = new Key(row, max);
    }

    public void withMinMax(Key min, Key max) {
        this.min = min;
        this.max = max;
    }

    /**
     * Writes a number of field index entries for the specified field and value, using the default datatype
     *
     * @param field
     *            the field
     * @param value
     *            the value
     * @param count
     *            the number of index entries to write
     */
    protected void writeData(String field, String value, int count) {
        writeData(field, value, DEFAULT_DATATYPE, count);
    }

    /**
     * Writes a number of field index entries for the specified field, value and datatype
     *
     * @param field
     *            the field
     * @param value
     *            the value
     * @param datatype
     *            the datatype
     * @param count
     *            the number of index entries to write
     */
    protected void writeData(String field, String value, String datatype, int count) {
        Preconditions.checkNotNull(data, "data should not be null");
        for (int i = 1; i <= count; i++) {
            int index = 1000 + i;
            Key key = new Key(row, "fi\0" + field, value + '\u0000' + datatype + '\u0000' + "uid-" + index, 10L);
            data.put(key, EMPTY_VALUE);
        }
    }

    /**
     * Writes a single uid index entry for the specified field, value, datatype
     *
     * @param field
     *            the field
     * @param value
     *            the value
     * @param datatype
     *            the datatype
     * @param uid
     *            the uid
     */
    protected void writeIndex(String field, String value, String datatype, int uid) {
        Preconditions.checkNotNull(data, "data should not be null");
        int index = 1000 + uid;
        Key key = new Key(row, "fi\0" + field, value + '\u0000' + datatype + '\u0000' + "uid-" + index, 10L);
        data.put(key, EMPTY_VALUE);
    }

    /**
     * Write an INCLUSIVE range of uids for the specified field and value, using the default datatype
     *
     * @param field
     *            the field
     * @param value
     *            the value
     * @param min
     *            the minimum uid
     * @param max
     *            the maximum uid
     */
    protected void writeRange(String field, String value, int min, int max) {
        writeRange(field, value, DEFAULT_DATATYPE, min, max);
    }

    /**
     * Write an INCLUSIVE range of uids for the specified field, value and datatype
     *
     * @param field
     *            the field
     * @param value
     *            the value
     * @param datatype
     *            the datatype
     * @param min
     *            the min
     * @param max
     *            the max
     */
    protected void writeRange(String field, String value, String datatype, int min, int max) {
        for (int i = min; i <= max; i++) {
            writeIndex(field, value, datatype, i);
        }
    }

    protected void drive() {
        // always clear results before each test iteration
        results.clear();

        BaseDocIdIterator iter = createIterator();

        if (!datatypes.isEmpty()) {
            iter.withDatatypes(datatypes);
        }

        if (timeFilter != null) {
            iter.withTimeFilter(timeFilter);
        }

        // set the min and max if provided. This simulates secondary scan bounded by an initial scan
        if (min != null && max != null) {
            iter.withMinMax(min, max);
        }

        while (iter.hasNext()) {
            Key key = iter.next();
            results.add(key);
        }

        stats = iter.getStats();
    }

    /**
     * Create the iterator according to the specific context
     *
     * @return a doc id iterator
     */
    protected abstract BaseDocIdIterator createIterator();

    /**
     * Creates a new source from the backing data and seeks the iterator.
     * <p>
     * Note: it is up to each caller to further restrict the scan range
     *
     * @return a SortedKeyValueIterator
     */
    protected SortedKeyValueIterator<Key,Value> createSource() {
        assertNotNull(data);
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);
        try {
            source.seek(new Range(), Collections.emptySet(), true);
        } catch (Exception e) {
            fail("Failed to init source");
        }
        return source;
    }

    /**
     * Utility method that parses a string query into a ASTJexlScript
     *
     * @param query
     *            the query string
     * @return the script
     */
    protected ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (Exception e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException(e);
        }
    }

    /**
     * Utility method to assert an expected number of results
     *
     * @param expected
     *            the expected number of results
     */
    protected void assertResultSize(int expected) {
        assertEquals(expected, results.size());
    }
}
