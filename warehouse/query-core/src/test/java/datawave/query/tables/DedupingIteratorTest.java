package datawave.query.tables;

import datawave.query.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DedupingIteratorTest {
    
    public static final int DUPS_LIST_SZ = 2160;
    public static final int DEDUPED_LIST_SZ = 1500;
    private static List<Map.Entry<Key,Value>> DUPS_LIST;
    
    private int bloomExpected;
    private double bloomFpp;
    
    @BeforeClass
    public static void beforeClass() {
        DUPS_LIST = new ArrayList<>(DUPS_LIST_SZ);
        Text commonCF = new Text("enwiki" + Constants.NULL_BYTE_STRING + "-376vy6.-ywf5yh.-3p9u87");
        
        // put a bunch of dups in the front
        for (int i = 1; i <= 20; i++) {
            DUPS_LIST.add(new TestEntry(new Key(new Text("20200101_" + i), commonCF)));
            DUPS_LIST.add(new TestEntry(new Key(new Text("20200101_" + i), commonCF)));
            DUPS_LIST.add(new TestEntry(new Key(new Text("20200101_" + i), commonCF)));
            DUPS_LIST.add(new TestEntry(new Key(new Text("20200101_" + i), commonCF)));
        }
        // unique in the middle, varying CF
        for (int i = 21; i <= 900; i++) {
            DUPS_LIST.add(new TestEntry(new Key(new Text("20200101_21"), new Text("enwiki" + Constants.NULL_BYTE_STRING + "-376vy6.-ywf5yh.-r3zz" + i))));
        }
        // more dups on the end
        for (int i = 901; i <= DEDUPED_LIST_SZ; i++) {
            DUPS_LIST.add(new TestEntry(new Key(new Text("20200101_" + i), commonCF)));
            DUPS_LIST.add(new TestEntry(new Key(new Text("20200101_" + i), commonCF)));
        }
    }
    
    @Before
    public void before() {
        bloomExpected = 2000;
        bloomFpp = 1e-15;
    }
    
    @Test
    public void test_nodups() {
        assertEquals(DUPS_LIST_SZ, DUPS_LIST.size());
        
        Iterable<Map.Entry<Key,Value>> input = () -> new DedupingIterator(DUPS_LIST.iterator(), bloomExpected, bloomFpp);
        
        List<Map.Entry<Key,Value>> output = new ArrayList<>();
        input.forEach(output::add);
        
        assertEquals(DEDUPED_LIST_SZ, output.size());
        
        // Uniqueness sanity check
        assertEquals(DEDUPED_LIST_SZ, new HashSet<>(output).size());
    }
    
    @Test
    public void test_bloomExpectedTooSmall() {
        bloomExpected = 200;
        
        assertEquals(DUPS_LIST_SZ, DUPS_LIST.size());
        
        Iterable<Map.Entry<Key,Value>> input = () -> new DedupingIterator(DUPS_LIST.iterator(), bloomExpected, bloomFpp);
        
        List<Map.Entry<Key,Value>> output = new ArrayList<>();
        input.forEach(output::add);
        
        // False positives should've prevented some entries from being included
        assertTrue(output.size() < DEDUPED_LIST_SZ);
    }
    
    private static class TestEntry implements Map.Entry<Key,Value> {
        
        private Key key;
        
        TestEntry(Key key) {
            this.key = key;
        }
        
        @Override
        public Key getKey() {
            return this.key;
        }
        
        @Override
        public Value getValue() {
            return null;
        }
        
        @Override
        public Value setValue(Value val) {
            return null;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TestEntry testEntry = (TestEntry) o;
            return key.equals(testEntry.key);
        }
        
        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }
}
