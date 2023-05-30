package datawave.query.iterator;

import com.google.common.base.Predicates;
import datawave.query.attributes.Document;
import datawave.query.iterator.aggregation.DocumentData;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccumuloTableIterableTest {
    
    private static final Value EMPTY_VALUE = new Value(new byte[0]);
    
    private final int numEvents = 3;
    private final int numFields = 5;
    
    private SortedMapIterator source;
    
    @Before
    public void beforeEach() {
        TreeMap<Key,Value> data = new TreeMap<>();
        for (int i = 0; i < numEvents; i++) {
            for (int j = 0; j < numFields; j++) {
                data.put(new Key("row", "datatype\u0000uid" + i, "FIELD_" + j + "\0value_" + j), EMPTY_VALUE);
            }
        }
        source = new SortedMapIterator(data);
    }
    
    @Test
    public void testSeek() throws IOException {
        AccumuloTableIterable iter = new AccumuloTableIterable(source, Predicates.alwaysTrue(), false, false);
        
        iter.seek(new Range(), Collections.emptySet(), false);
        
        Iterator<Map.Entry<DocumentData,Document>> data = iter.iterator();
        assertTrue(data.hasNext());
        Key k = data.next().getKey().getKey();
        assertEquals(new Key("row", "datatype\u0000uid0", "FIELD_0\u0000value_0"), k);
        
        // second seek
        iter.seek(new Range(k, false, null, false), Collections.emptySet(), false);
        assertTrue(data.hasNext());
        k = data.next().getKey().getKey();
        assertEquals(new Key("row", "datatype\u0000uid1", "FIELD_0\u0000value_0"), k);
    }
}
