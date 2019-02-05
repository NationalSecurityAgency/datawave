package datawave.ingest.mapreduce.job;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import org.apache.accumulo.core.data.Key;

public class BulkIngestKeyTest {
    
    @Test
    public void testKeySerialization() throws IOException {
        Text tableName = new Text("testTable");
        Key key = new Key(new Text("row\0key"), new Text("colFam"), new Text("col\0qual"), new Text("col\0vis"));
        BulkIngestKey expected = new BulkIngestKey(tableName, key);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        expected.write(dos);
        
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        DataInputStream dis = new DataInputStream(bis);
        BulkIngestKey actual = new BulkIngestKey();
        actual.readFields(dis);
        
        assertEquals(0, expected.compareTo(actual));
    }
    
    @Test
    public void testBinaryKeyComparison() throws IOException {
        Text tableName = new Text("testTable");
        Key key = new Key(new Text("row\0key"), new Text("e"), new Text("col\0qual\0data"), new Text("sample\0col\0vis"));
        key.setDeleted(true);
        BulkIngestKey key1 = new BulkIngestKey(tableName, key);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        key1.write(dos);
        byte[] key1bytes = bos.toByteArray();
        
        key = new Key(new Text("row\0key"), new Text("h"), new Text("col"), new Text("sample\0col\0vis"));
        key.setDeleted(false);
        BulkIngestKey key2 = new BulkIngestKey(tableName, key);
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
        key2.write(dos);
        byte[] key2bytes = bos.toByteArray();
        
        BulkIngestKey.Comparator comparator = new BulkIngestKey.Comparator();
        int result = comparator.compare(key1bytes, 0, key1bytes.length, key2bytes, 0, key2bytes.length);
        assertTrue(result < 0);
        
        result = comparator.compare(key2bytes, 0, key2bytes.length, key1bytes, 0, key1bytes.length);
        assertFalse(result < 0);
        
        // for completeness...
        result = comparator.compare(key2bytes, 0, key2bytes.length, key2bytes, 0, key2bytes.length);
        assertEquals(0, result);
        
        key = new Key(new Text("row\0key"), new Text("e"), new Text("col\0qual\0data"), new Text("sample\0col\0vis"));
        key.setDeleted(true);
        key1 = new BulkIngestKey(tableName, key);
        
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
        key1.write(dos);
        key1bytes = bos.toByteArray();
        
        key.setDeleted(false);
        key2 = new BulkIngestKey(tableName, key);
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
        key2.write(dos);
        key2bytes = bos.toByteArray();
        
        result = comparator.compare(key1bytes, 0, key1bytes.length, key2bytes, 0, key2bytes.length);
        assertTrue(0 > result);
        
        key = new Key(new Text("row\0key"), new Text("e"), new Text("col\0qual\0data"), new Text("sample\0col\0vis"));
        key.setDeleted(false);
        key1 = new BulkIngestKey(tableName, key);
        
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
        key1.write(dos);
        key1bytes = bos.toByteArray();
        
        key.setDeleted(true);
        key2 = new BulkIngestKey(tableName, key);
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
        key2.write(dos);
        key2bytes = bos.toByteArray();
        
        result = comparator.compare(key1bytes, 0, key1bytes.length, key2bytes, 0, key2bytes.length);
        assertTrue(0 < result);
        
    }
    
    @Test
    public void testBinaryKeyTimestampComparison() throws IOException {
        Text tableName = new Text("testTable");
        Key key = new Key(new Text("20100817_23"), new Text("col\0fam"), new Text("col\0qual"), new Text("another\0col\0vis"), 1282048383952L);
        BulkIngestKey key1 = new BulkIngestKey(tableName, key);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        key1.write(dos);
        byte[] key1bytes = bos.toByteArray();
        
        key = new Key(new Text("20100817_23"), new Text("col\0fam"), new Text("col\0qual"), new Text("another\0col\0vis"), 1282046898252L);
        BulkIngestKey key2 = new BulkIngestKey(tableName, key);
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
        key2.write(dos);
        byte[] key2bytes = bos.toByteArray();
        
        assertTrue(key1.compareTo(key2) < 0);
        
        BulkIngestKey.Comparator comparator = new BulkIngestKey.Comparator();
        int result = comparator.compare(key1bytes, 0, key1bytes.length, key2bytes, 0, key2bytes.length);
        assertTrue(result < 0);
        
        result = comparator.compare(key2bytes, 0, key2bytes.length, key1bytes, 0, key1bytes.length);
        assertFalse(result < 0);
    }
    
    @Test
    public void testEquals() {
        Text tableName1 = new Text("testTable");
        Key key1 = new Key(new Text("row\0key"), new Text("colFam"), new Text("col\0qual"), new Text("col\0vis"));
        BulkIngestKey bik1 = new BulkIngestKey(tableName1, key1);
        
        Text tableName2 = new Text("testTable");
        Key key2 = new Key(new Text("row\0key"), new Text("colFam"), new Text("col\0qual"), new Text("col\0vis"));
        BulkIngestKey bik2 = new BulkIngestKey(tableName2, key2);
        
        assertEquals(bik1, bik2);
        
        bik2 = new BulkIngestKey(new Text("differentTableName"), key2);
        assertFalse(bik1.equals(bik2));
        
        bik2 = new BulkIngestKey(tableName2, new Key());
        assertFalse(bik1.equals(bik2));
        
        // for completeness...
        assertTrue(bik1.equals(bik1));
        assertFalse(bik1.equals(null));
        assertFalse(bik1.toString().equals(""));
    }
    
    @Test
    public void testHashCode() {
        Text tableName1 = new Text("testTable");
        Key key1 = new Key(new Text("row\0key"), new Text("colFam"), new Text("col\0qual"), new Text("col\0vis"));
        BulkIngestKey bik1 = new BulkIngestKey(tableName1, key1);
        
        Text tableName2 = new Text("testTable");
        Key key2 = new Key(new Text("row\0key"), new Text("colFam"), new Text("col\0qual"), new Text("col\0vis"));
        BulkIngestKey bik2 = new BulkIngestKey(tableName2, key2);
        
        assertEquals(bik1.hashCode(), bik2.hashCode());
        
        bik2 = new BulkIngestKey(new Text("differentTableName"), key2);
        assertNotEquals(bik1.hashCode(), bik2.hashCode());
        
        bik2 = new BulkIngestKey(tableName2, new Key());
        assertNotEquals(bik1.hashCode(), bik2.hashCode());
        
        // for completeness...
        bik2 = new BulkIngestKey(tableName2, null);
        assertNotEquals(bik1.hashCode(), bik2.hashCode());
    }
    
}
