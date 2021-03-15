package datawave.query.iterator.logic;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class IndexIteratorTest {
    
    @Test
    public void testSimpleIndexScan() throws IOException {
        String field = "FIELD_A";
        String value = "value_a";
        SortedMapIterator source = new SortedMapIterator(buildFiData(field, value));
        
        IndexIterator iter = IndexIterator.builder(new Text(field), new Text(value), source).build();
        iter.seek(new Range(), null, false);
        
        int i = 0;
        while (iter.hasTop()) {
            Key tk = iter.getTopKey();
            assertEquals("datatype\0uid" + i, tk.getColumnFamily().toString());
            iter.next();
            i++;
        }
        assertEquals(10, i);
    }
    
    private TreeMap<Key,Value> buildFiData(String field, String value) {
        Value blank = new Value(new byte[0]);
        TreeMap<Key,Value> data = new TreeMap<>();
        for (int i = 0; i < 10; i++) {
            data.put(new Key("row", "fi\0" + field, value + "\0datatype\0uid" + i), blank);
        }
        return data;
    }
}
