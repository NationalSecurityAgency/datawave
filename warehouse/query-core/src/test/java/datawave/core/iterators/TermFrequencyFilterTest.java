package datawave.core.iterators;

import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.postprocessing.tf.TermOffsetPopulator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static datawave.query.Constants.NULL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Copy of {@link TermFrequencyIteratorTest} but validates {@link TermFrequencyFilter}
 */
public class TermFrequencyFilterTest {

    private final String lowers = "abcdefghijklmnopqrstuvwxyz";
    private final String uppers = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static final String TF = "tf";
    private static final String ROW = "row";
    private static final String DT = "type1";

    @Test
    public void testDocRange_singleKey_parent() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid3\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid3\0value_c\0FIELD_A"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    public void testDocRange_singleKey_parent_multiField() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid3\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid3\0value_a\0FIELD_C"));
        searchSpace.add(new Text("datatype\0uid3\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid3\0value_c\0FIELD_C"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(4, count);
    }

    @Test
    public void testDocRange_singleKey_child() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid3.3\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid3.3\0value_c\0FIELD_A"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    public void testDocRange_singleKey_child_multiField() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid3.3\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid3.3\0value_a\0FIELD_C"));
        searchSpace.add(new Text("datatype\0uid3.3\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid3.3\0value_c\0FIELD_C"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(4, count);
    }

    @Test
    public void testDocRange_minMaxKeys() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.1\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.1\0value_c\0FIELD_A"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(4, count);
    }

    @Test
    public void testDocRange_rotatingChildKeys() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.1\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.1\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.2\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.2\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.3\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.3\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.4\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.4\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.5\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.5\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.6\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.6\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.7\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.7\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.8\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.8\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.9\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.9\0value_c\0FIELD_A"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(20, count);
    }

    @Test
    public void testParentFirstChildAggregation() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.1\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0.1\0value_c\0FIELD_A"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(4, count);
    }

    // Given an overly broad range, does the iterator still traverse the minimum set of keys necessary
    @Test
    public void testFullScanRange_singleKey_first() throws IOException {
        Key start = new Key("20200314_0", TF, "datatype\0uid0");
        Key end = new Key("20200314_0", TF, "datatype\0uid9.9\uffff");
        Range r = new Range(start, false, end, true);

        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0\0value_c\0FIELD_A"));

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(2, count);
    }

    // Given an overly broad range, does the iterator still traverse the minimum set of keys necessary
    @Test
    public void testFullScanRange_singleKey_middle() throws IOException {
        Key start = new Key("20200314_0", TF, "datatype\0uid0");
        Key end = new Key("20200314_0", TF, "datatype\0uid9.9\uffff");
        Range r = new Range(start, false, end, true);

        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid5.5\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid5.5\0value_c\0FIELD_A"));

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    public void testFullScanRange_minMaxKeys() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid9.9\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid9.9\0value_c\0FIELD_A"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(4, count);
    }

    @Test
    public void testFullScanRange_rotatingSingleKeyPerParent() throws IOException {
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid0\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid1.1\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid1.1\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid2.2\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid2.2\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid3.3\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid3.3\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid4.4\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid4.4\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid5.5\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid5.5\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid6.6\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid6.6\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid7.7\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid7.7\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid8.8\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid8.8\0value_c\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid9.9\0value_a\0FIELD_A"));
        searchSpace.add(new Text("datatype\0uid9.9\0value_c\0FIELD_A"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);

        TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
        tfIter.init(createSource(), null, null);
        tfIter.seek(r, null, true);

        assertNotNull(tfIter);
        assertTrue(tfIter.hasTop());
        int count = 0;
        while (tfIter.hasTop()) {
            tfIter.next();
            count++;
        }
        assertEquals(20, count);
    }

    // Roll through every field in the search space and assert proper hit count
    @Test
    public void testSearchEveryField() throws IOException {
        TreeMap<Key,Value> data = createData();

        for (char upper : uppers.toCharArray()) {
            String field = "FIELD_" + upper;
            TreeSet<Text> searchSpace = buildFullSearchSpace(field, "value_a", "value_b");
            Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text("20200314_0"), searchSpace);
            TermFrequencyFilter tfIter = new TermFrequencyFilter(searchSpace);
            tfIter.init(createSourceFromData(data), null, null);
            tfIter.seek(r, null, true);

            assertNotNull(tfIter);
            assertTrue(tfIter.hasTop());
            Set<Key> hits = new HashSet<>();
            while (tfIter.hasTop()) {
                hits.add(tfIter.getTopKey());
                tfIter.next();
            }
            assertEquals("Expected to get 20 hits for field " + field + "but was " + hits.size(), 20, hits.size());
        }
    }

    private TreeSet<Text> buildFullSearchSpace(String field, String valueA, String valueB) {
        String[] uids = new String[] {"uid0", "uid1.1", "uid2.2", "uid3.3", "uid4.4", "uid5.5", "uid6.6", "uid7.7", "uid8.8", "uid9.9"};
        TreeSet<Text> searchSpace = new TreeSet<>();
        for (String uid : uids) {
            searchSpace.add(new Text("datatype\0" + uid + "\0" + valueA + "\0" + field));
            searchSpace.add(new Text("datatype\0" + uid + "\0" + valueB + "\0" + field));
        }
        return searchSpace;
    }

    @Test
    public void testGetNextSeekRange() throws IOException {
        SortedKeyValueIterator<Key,Value> source = buildSourceWithNullValues();
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("type1\u0000123.345.456\0value_c\0FIELD_A"));
        searchSpace.add(new Text("type1\u0000123.345.456\0value_d\0FIELD_A"));
        searchSpace.add(new Text("type1\u0000123.345.456.1\0value_c\0FIELD_A"));
        searchSpace.add(new Text("type1\u0000123.345.456.1\0value_d\0FIELD_A"));

        Range r = TermOffsetPopulator.getRangeFromSearchSpace(new Text(ROW), searchSpace);

        TermFrequencyFilter iter = new TermFrequencyFilter(searchSpace);
        iter.init(source, null, null);
        iter.seek(r, null, true);

        // Hit the first key
        assertNotNull(iter);
        assertTrue(iter.hasTop());
        int count = 0;
        while (iter.hasTop()) {
            iter.next();
            count++;
        }
        // 123.345.456\0value_d\0FIELD_A is malformed, thus skipped
        // 123.345.456.1\0value_c\0FIELD_A is malformed, thus skipped
        assertEquals(2, count);
    }

    // Create data iter.
    private SortedListKeyValueIterator createSource() {
        return createSourceFromData(createData());
    }

    private SortedListKeyValueIterator createSourceFromData(TreeMap<Key,Value> data) {
        return new SortedListKeyValueIterator(data);
    }

    public TreeMap<Key,Value> createData() {

        // Build TF keys like 'row:tf:datatype\0uid\0fieldValue\0fieldName'
        Text row = new Text("20200314_0");
        Text cf = new Text(TF);

        // Generates 67.6k keys
        TreeMap<Key,Value> data = new TreeMap<>();
        for (int ii = 0; ii < 10; ii++) {
            for (int jj = 0; jj < 10; jj++) {
                for (int lower = 0; lower < lowers.length(); lower++) {
                    for (int upper = 0; upper < uppers.length(); upper++) {
                        String uid = "uid" + ii;
                        if (jj > 0) {
                            uid += "." + jj;
                        }
                        String value = "value_" + lowers.charAt(lower);
                        String field = "FIELD_" + uppers.charAt(upper);
                        String uidValueField = uid + '\u0000' + value + '\u0000' + field;
                        Text cq = new Text("datatype\0" + uidValueField);

                        Key k = new Key(row, cf, cq);
                        data.put(k, new Value());
                    }
                }
            }
        }
        return data;
    }

    // Build some basic TF keys with values
    public SortedListKeyValueIterator buildSourceWithNullValues() {
        List<Map.Entry<Key,Value>> baseSource = new ArrayList<>();

        baseSource.add(new SimpleEntry<>(getTfKey(ROW, DT, "123.345.456", "FIELD_A", "value_a"), new Value()));
        baseSource.add(new SimpleEntry<>(getTfKey(ROW, DT, "123.345.456", "FIELD_A", "value_b"), new Value()));
        baseSource.add(new SimpleEntry<>(getTfKey(ROW, DT, "123.345.456", "FIELD_A", "value_c"), new Value()));
        baseSource.add(new SimpleEntry<>(getTfKey(ROW, DT, "123.345.456", "FIELD_A", "val\0ue_d"), new Value()));
        baseSource.add(new SimpleEntry<>(getTfKey(ROW, DT, "123.345.456.1", "FIELD_A", "value_a"), new Value()));
        baseSource.add(new SimpleEntry<>(getTfKey(ROW, DT, "123.345.456.1", "FIELD_A", "value_b"), new Value()));
        baseSource.add(new SimpleEntry<>(getTfKey(ROW, DT, "123.345.456.1", "FIELD_A", "val\0ue_c"), new Value()));
        baseSource.add(new SimpleEntry<>(getTfKey(ROW, DT, "123.345.456.1", "FIELD_A", "value_d"), new Value()));

        return new SortedListKeyValueIterator(baseSource);
    }

    private Key getTfKey(String row, String dataType, String uid, String fieldName, String fieldValue) {
        return new Key(row, TF, dataType + NULL + uid + NULL + fieldValue + NULL + fieldName);
    }
}
