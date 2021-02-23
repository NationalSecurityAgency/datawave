package datawave.core.iterators;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.query.iterator.SortedListKeyValueIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.StringUtils;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TermFrequencyIteratorTest {
    
    private String lowers = "abcdefghijklmnopqrstuvwxyz";
    private String uppers = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    
    @Test
    public void testDocRange_singleKey_parent() throws IOException {
        Key start = new Key("20200314_0", "tf", "datatype\0uid3");
        Key end = new Key("20200314_0", "tf", "datatype\0uid3\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid3")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        Key start = new Key("20200314_0", "tf", "datatype\0uid3");
        Key end = new Key("20200314_0", "tf", "datatype\0uid3\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid3")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        Multimap<String,String> moreFields = buildFieldValues("FIELD_C", "value_a", "value_c");
        fieldValues.putAll(moreFields);
        
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        Key start = new Key("20200314_0", "tf", "datatype\0uid3");
        Key end = new Key("20200314_0", "tf", "datatype\0uid3\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid3.3")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        Key start = new Key("20200314_0", "tf", "datatype\0uid3");
        Key end = new Key("20200314_0", "tf", "datatype\0uid3\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid3.3")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        Multimap<String,String> moreFields = buildFieldValues("FIELD_C", "value_a", "value_c");
        fieldValues.putAll(moreFields);
        
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        Key start = new Key("20200314_0", "tf", "datatype\0uid0");
        Key end = new Key("20200314_0", "tf", "datatype\0uid0.1\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.1")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        Key start = new Key("20200314_0", "tf", "datatype\0uid0");
        Key end = new Key("20200314_0", "tf", "datatype\0uid0\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.1")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.2")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.3")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.4")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.5")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.6")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.7")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.8")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.9")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        Key start = new Key("20200314_0", "tf", "datatype\0uid0");
        Key end = new Key("20200314_0", "tf", "datatype\0uid0.1\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0.1")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
    public void testFullScanRange_singleKey_first() throws IOException {
        Key start = new Key("20200314_0", "tf", "datatype\0uid0");
        Key end = new Key("20200314_0", "tf", "datatype\0uid9.9\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
    public void testFullScanRange_singleKey_middle() throws IOException {
        Key start = new Key("20200314_0", "tf", "datatype\0uid0");
        Key end = new Key("20200314_0", "tf", "datatype\0uid9.9\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid5.5")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        Key start = new Key("20200314_0", "tf", "datatype\0uid0");
        Key end = new Key("20200314_0", "tf", "datatype\0uid9.9\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid9.9")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        Key start = new Key("20200314_0", "tf", "datatype\0uid0");
        Key end = new Key("20200314_0", "tf", "datatype\0uid9.9\uffff");
        Range r = new Range(start, false, end, true);
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid1.1")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid2.2")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid3.3")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid4.4")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid5.5")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid6.6")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid7.7")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid8.8")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid9.9")));
        
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_a", "value_c");
        TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
        // Full scan range
        Key start = new Key("20200314_0", "tf", "datatype\0uid0");
        Key end = new Key("20200314_0", "tf", "datatype\0uid9.9\uffff");
        Range r = new Range(start, false, end, true);
        
        // Hit in every doc
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid0")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid1.1")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid2.2")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid3.3")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid4.4")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid5.5")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid6.6")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid7.7")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid8.8")));
        keys.add(new Key(new Text("20200314_0"), new Text("datatype\u0000uid9.9")));
        
        TreeMap<Key,Value> data = createData();
        
        for (char upper : uppers.toCharArray()) {
            String field = "FIELD_" + upper;
            Multimap<String,String> fieldValues = buildFieldValues(field, "value_a", "value_c");
            TermFrequencyIterator tfIter = new TermFrequencyIterator(fieldValues, keys);
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
    
    @Test
    public void testGetNextSeekRange() throws IOException {
        SortedKeyValueIterator<Key,Value> tfIter = buildIterAcrossValuesWithNulls();
        Multimap<String,String> fieldValues = buildFieldValues("FIELD_A", "value_c", "value_d");
        
        Set<Key> keys = new TreeSet<>();
        keys.add(new Key(new Text("row"), new Text("type1\u0000123.345.456")));
        keys.add(new Key(new Text("row"), new Text("type1\u0000123.345.456.1")));
        
        TermFrequencyIterator iter = new TermFrequencyIterator(fieldValues, keys);
        iter.init(tfIter, null, null);
        
        Key start = getTfKey("row", "type1", "123.345.456", "FIELD_A", "value_c");
        Key end = getTfKey("row", "type1", "123.345.456.1", "FIELD_A", "value_d");
        Range r = new Range(start, true, end, false);
        
        // Hit the first key
        iter.seek(r, null, true);
        
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
    public void testTreeSetHigher() {
        TreeSet<String> values = Sets.newTreeSet(Lists.newArrayList("value_a", "value_b", "value_c"));
        assertEquals("value_a", values.higher("value"));
        assertEquals("value_b", values.higher("value_a"));
        assertEquals("value_c", values.higher("value_b"));
        assertNull(values.higher("value_c"));
        assertNull(values.higher("value_zz"));
    }
    
    @Test
    public void testGetDistance() {
        TermFrequencyIterator iter = new TermFrequencyIterator();
        
        assertEquals(1.0d, iter.getDistance("a", "b"), 0.01d);
        assertEquals(0.5d, iter.getDistance("aa", "ab"), 0.01d);
        assertEquals(0.33d, iter.getDistance("aaa", "aab"), 0.01d);
        assertEquals(0.25d, iter.getDistance("aaaa", "aaab"), 0.01d);
        assertEquals(0.2d, iter.getDistance("aaaaa", "aaaab"), 0.01d);
        assertEquals(0.16d, iter.getDistance("aaaaaa", "aaaaab"), 0.01d);
        assertEquals(0.14d, iter.getDistance("aaaaaaa", "aaaaaab"), 0.01d);
        assertEquals(1.0d, iter.getDistance("a", "z"), 0.01d);
        assertEquals(0.14d, iter.getDistance("aaaaaaa", "aaaaaaz"), 0.01d);
        
        assertEquals(0.14d, iter.getDistance("value_a", "value_b"), 0.01d);
        assertEquals(0.14d, iter.getDistance("value_a", "value_z"), 0.01d);
        assertEquals(0.14d, iter.getDistance("value_", "value_zzz"), 0.01d);
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
        Text cf = new Text("tf");
        
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
    public SortedListKeyValueIterator buildIterAcrossValuesWithNulls() {
        List<Map.Entry<Key,Value>> baseSource = new ArrayList<>();
        
        baseSource.add(new SimpleEntry(getTfKey("row", "type1", "123.345.456", "FIELD_A", "value_a"), new Value()));
        baseSource.add(new SimpleEntry(getTfKey("row", "type1", "123.345.456", "FIELD_A", "value_b"), new Value()));
        baseSource.add(new SimpleEntry(getTfKey("row", "type1", "123.345.456", "FIELD_A", "value_c"), new Value()));
        baseSource.add(new SimpleEntry(getTfKey("row", "type1", "123.345.456", "FIELD_A", "val\0ue_d"), new Value()));
        baseSource.add(new SimpleEntry(getTfKey("row", "type1", "123.345.456.1", "FIELD_A", "value_a"), new Value()));
        baseSource.add(new SimpleEntry(getTfKey("row", "type1", "123.345.456.1", "FIELD_A", "value_b"), new Value()));
        baseSource.add(new SimpleEntry(getTfKey("row", "type1", "123.345.456.1", "FIELD_A", "val\0ue_c"), new Value()));
        baseSource.add(new SimpleEntry(getTfKey("row", "type1", "123.345.456.1", "FIELD_A", "value_d"), new Value()));
        
        return new SortedListKeyValueIterator(baseSource);
    }
    
    private Key getTfKey(String row, String dataType, String uid, String fieldName, String fieldValue) {
        return new Key(row, "tf", dataType + NULL + uid + NULL + fieldValue + NULL + fieldName);
    }
    
    public Multimap<String,String> buildFieldValues(String field, String... values) {
        Multimap<String,String> fieldValues = HashMultimap.create();
        for (String value : values) {
            fieldValues.put(field, value);
        }
        return fieldValues;
    }
    
    @Test
    public void testGetValueFromParts() {
        // CQ = datatype\0uid\0fieldValue\0fieldName
        TermFrequencyIterator iter = new TermFrequencyIterator();
        
        // Full CQ
        String cq = "datatype\0uid\0fieldValue\0fieldName";
        assertEquals("fieldValue", iter.getValueFromParts(StringUtils.split(cq, '\0')));
        
        // CQ less field name
        cq = "datatype\0uid\0fieldValue";
        assertEquals("fieldValue", iter.getValueFromParts(StringUtils.split(cq, '\0')));
        
        // CQ just datatype and uid
        cq = "datatype\0uid";
        assertNull(iter.getValueFromParts(StringUtils.split(cq, '\0')));
        
        // CQ where value has multiple null bytes
        cq = "datatype\0uid\0fi\0eld\0Va\0lue\0fieldName";
        assertEquals("fi\0eld\0Va\0lue", iter.getValueFromParts(StringUtils.split(cq, '\0')));
    }
}
