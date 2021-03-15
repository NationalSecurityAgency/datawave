package datawave.query.iterator.logic;

import datawave.query.iterator.NestedIterator;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexIteratorBridgeTest {
    
    @Test
    public void testSimpleScan() {
        String field = "FIELD_A";
        String value = "value_a";
        JexlNode node = JexlNodeFactory.buildEQNode(field, value);
        SortedMapIterator source = new SortedMapIterator(buildFiData(field, value, 0, 10));
        
        IndexIterator iter = IndexIterator.builder(new Text(field), new Text(value), source).build();
        
        IndexIteratorBridge iib = new IndexIteratorBridge(iter, node, field);
        iib.seek(new Range(), null, false);
        
        int i = 0;
        while (iib.hasNext()) {
            Key tk = iib.next();
            assertEquals("datatype\0uid" + i, tk.getColumnFamily().toString());
            
            // Assert that a call to leaves() -> getTopKey() will correctly return the current top key
            
            NestedIterator<Key> leaf = iib.leaves().iterator().next();
            assertTrue(leaf instanceof IndexIteratorBridge);
            
            i++;
        }
        assertEquals(10, i);
    }
    
    /**
     * Builds hits for the field-value pairt, start-stop represent the document range
     */
    private TreeMap<Key,Value> buildFiData(String field, String value, int start, int stop) {
        Value blank = new Value(new byte[0]);
        TreeMap<Key,Value> data = new TreeMap<>();
        for (int i = start; i < stop; i++) {
            data.put(new Key("row", "fi\0" + field, value + "\0datatype\0uid" + i), blank);
        }
        return data;
    }
}
