package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;

class IndexIteratorTest {

    private static final Value EMPTY_VALUE = new Value();

    private final SortedSet<String> uids = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));

    @Test
    void testIteration() throws IOException {
        IndexIterator itr = createIndexIterator("FIELD_A", uids);
        driveIterator(itr, uids, "FIELD_A");
    }

    @Test
    void testNoInit() {
        IndexIterator itr = createIndexIterator("FIELD_A", uids);
        assertThrows(UnsupportedOperationException.class, () -> itr.init(null, null, null));
    }

    @Test
    void testIterationInterrupted() {
        IndexIterator itr = createInterruptibleIndexIterator("FIELD_A", uids);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids, "FIELD_A"));
    }

    @Test
    void testInterruptOnSecondNext() throws IOException {
        IndexIterator itr = createInterruptibleIndexIterator("FIELD_A", uids, 2);
        itr.seek(new Range(), Collections.emptyList(), false);

        assertTrue(itr.hasTop());
        itr.next();

        assertTrue(itr.hasTop());
        assertThrows(IterationInterruptedException.class, itr::next);
    }

    @Test
    void testIndexOnlyIterator() throws IOException {
        IndexIterator itr = createIndexIterator("FIELD_A", uids, true);
        driveIterator(itr, uids, "FIELD_A");
    }

    // === assert methods ===

    /**
     * Drive an iterator and assert document correctness for the field and uids
     *
     * @param itr
     *            the IndexIterator
     * @param field
     *            the field
     * @param uids
     *            the list of uids
     * @throws IOException
     *             if an exception is encountered
     */
    private void driveIterator(IndexIterator itr, SortedSet<String> uids, String field) throws IOException {
        // setup iterator
        itr.seek(new Range(), Collections.emptyList(), false);

        for (String uid : uids) {
            assertTrue(itr.hasTop());

            // not doing anything with top keys
            Key tk = itr.getTopKey();
            assertTopKey(tk, uid);

            Document d = itr.document();
            if (itr.buildDocument) {
                // assert field and uid in the returned document
                assertDocumentField(field, d);
                assertDocumentUid(uid, d);
            } else {
                assertEquals(0, d.size());
            }

            itr.next();
        }

        assertFalse(itr.hasTop());
    }

    /**
     * Assert the document contains the expected field
     *
     * @param field
     *            the expected field
     * @param d
     *            the document
     */
    protected static void assertDocumentField(String field, Document d) {
        assertTrue(d.containsKey(field), "document did not contain expected field: " + field);
    }

    /**
     * Assert the document contains the expected uid
     *
     * @param uid
     *            the expected uid
     * @param d
     *            the document
     */
    protected static void assertDocumentUid(String uid, Document d) {
        assertTrue(d.containsKey(Document.DOCKEY_FIELD_NAME), "Document did not contain a RECORD_ID");
        Attribute<?> attr = d.get(Document.DOCKEY_FIELD_NAME);
        Key metadata = attr.getMetadata();
        assertTopKey(metadata, uid);
    }

    protected static void assertTopKey(Key tk, String uid) {
        String cf = tk.getColumnFamily().toString();
        assertEquals(uid, cf.split("\0")[1]);
    }

    // === helper methods ===

    protected static IndexIterator createIndexIterator(String field, SortedSet<String> uids) {
        return createIndexIterator(field, uids, false);
    }

    protected static IndexIterator createIndexIterator(String field, SortedSet<String> uids, boolean buildDocument) {
        return createIterator(field, uids, buildDocument, -1);
    }

    protected static IndexIterator createInterruptibleIndexIterator(String field, SortedSet<String> uids) {
        return createInterruptibleIndexIterator(field, uids, false, 0);
    }

    protected static IndexIterator createInterruptibleIndexIterator(String field, SortedSet<String> uids, boolean buildDocument) {
        return createInterruptibleIndexIterator(field, uids, buildDocument, 0);
    }

    protected static IndexIterator createInterruptibleIndexIterator(String field, SortedSet<String> uids, boolean buildDocument, int maxIterations) {
        return createIterator(field, uids, buildDocument, maxIterations);
    }

    protected static IndexIterator createInterruptibleIndexIterator(String field, SortedSet<String> uids, int maxIterations) {
        return createIterator(field, uids, false, maxIterations);
    }

    private static IndexIterator createIterator(String field, SortedSet<String> uids, boolean buildDocument, int maxIterations) {
        SortedKeyValueIterator<Key,Value> source;
        if (maxIterations == -1) {
            source = createSource(field, uids);
        } else {
            source = createInterruptibleSource(field, uids, maxIterations);
        }

        Text textField = new Text(field);
        Text textValue = new Text("value");
        //  @formatter:off
        return IndexIterator.builder(textField, textValue, source)
                        //  always build documents for tests so we can assert results
                        .shouldBuildDocument(buildDocument)
                        .build();
        //  @formatter:on
    }

    protected static SortedKeyValueIterator<Key,Value> createSource(String field, SortedSet<String> uids) {
        SortedMap<Key,Value> sourceData = createSourceData(field, uids);
        return new SortedMapIterator(sourceData);
    }

    protected static SortedKeyValueIterator<Key,Value> createInterruptibleSource(String field, SortedSet<String> uids, int maxIterations) {
        SortedMap<Key,Value> sourceData = createSourceData(field, uids);
        return new InterruptibleSortedMapIterator(sourceData, maxIterations);
    }

    protected static SortedMap<Key,Value> createSourceData(String field, SortedSet<String> uids) {
        SortedMap<Key,Value> source = new TreeMap<>();
        for (String uid : uids) {
            source.put(createFiKey(field, uid), EMPTY_VALUE);
        }
        return source;
    }

    protected static Key createFiKey(String field, String uid) {
        String row = "20220314_17";
        String cf = "fi\0" + field;
        String cq = "value\0datatype\0" + uid;
        return new Key(row, cf, cq);
    }

    /**
     * Helper class that will throw an {@link IterationInterruptedException} after reaching the preconfigured number of max iterations.
     * <p>
     * When using this class be sure that the backing data contains at least three elements.
     */
    static class InterruptibleSortedMapIterator extends SortedMapIterator {

        private int count = 0;
        private final int maxIterations;

        /**
         * Constructor with a default max iterations of 3. This number was selected due to the implicit peeking iterator interface used by the
         * {@link IndexIteratorBridge} which requires at least a 'previous' and 'next' value.
         *
         * @param map
         *            backing data
         */
        public InterruptibleSortedMapIterator(SortedMap<Key,Value> map) {
            this(map, 3);
        }

        /**
         * Constructor that allows configuring max iterations
         *
         * @param map
         *            backing data
         * @param maxIterations
         *            max iterations before an exception is thrown
         */
        public InterruptibleSortedMapIterator(SortedMap<Key,Value> map, int maxIterations) {
            super(map);
            this.maxIterations = maxIterations;
        }

        @Override
        public void next() throws IOException {
            super.next();

            if (++count > maxIterations) {
                throw new IterationInterruptedException("throwing exception for tests");
            }
        }
    }
}
