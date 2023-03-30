package datawave.query.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DocumentDataIteratorTest {
    
    private static final TreeMap<Key,Value> data = new TreeMap<>();
    private static final Value EMPTY_VALUE = new Value(new byte[0]);
    private static final String datatype = "datatype";
    
    private static final int numEvents = 5;
    private static final int numFields = 5;
    
    @BeforeClass
    public static void setup() {
        data.put(new Key("row"), EMPTY_VALUE);
        data.put(new Key("row", datatype), EMPTY_VALUE);
        for (int i = 0; i < numEvents; i++) {
            for (int j = 0; j < numFields; j++) {
                data.put(new Key("row", datatype + "\0" + i, "FIELD_" + j + "\0" + i * j), EMPTY_VALUE);
            }
        }
    }
    
    @Test
    public void testIsEventKey() {
        Iterator<Map.Entry<Key,Value>> iter = data.entrySet().iterator();
        WrappedDocumentDataIterator documentIter = new WrappedDocumentDataIterator(new SortedMapIterator(data), new Range(), false, false);
        
        // first two entries are not event keys
        assertFalse(documentIter.isEventKey(iter.next().getKey()));
        assertFalse(documentIter.isEventKey(iter.next().getKey()));
        
        // next five should be event keys
        int totalKeys = numEvents * numFields;
        for (int i = 0; i < totalKeys; i++) {
            assertTrue(documentIter.isEventKey(iter.next().getKey()));
        }
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testFindAllEvent() {
        
        SortedSet<Key> expectedDocumentKeys = new TreeSet<>();
        for (int i = 0; i < numEvents; i++) {
            expectedDocumentKeys.add(new Key("row", datatype + "\0" + i, "FIELD_0" + "\0" + 0));
        }
        
        WrappedDocumentDataIterator documentIter = new WrappedDocumentDataIterator(new SortedMapIterator(data), new Range(), false, false);
        Iterator<Key> expectedIter = expectedDocumentKeys.iterator();
        
        while (documentIter.hasNext() && expectedIter.hasNext()) {
            Key expected = expectedIter.next();
            assertEquals(expected, documentIter.next().getKey());
        }
        
        assertFalse(documentIter.hasNext());
        assertFalse(expectedIter.hasNext());
    }
    
    private static class WrappedDocumentDataIterator extends DocumentDataIterator {
        
        public WrappedDocumentDataIterator(SortedKeyValueIterator<Key,Value> source, Range totalRange, boolean includeChildCount, boolean includeParent) {
            super(source, totalRange, includeChildCount, includeParent);
        }
        
        @Override
        protected boolean isEventKey(Key k) {
            return super.isEventKey(k);
        }
    }
    
}
