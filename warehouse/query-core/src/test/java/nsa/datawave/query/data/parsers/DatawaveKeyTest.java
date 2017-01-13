package nsa.datawave.query.data.parsers;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class DatawaveKeyTest {
    @Test
    public void parseShardDocKey() {
        Key testKey = new Key("row", "datatype\u0000uid", "fieldNameA\u0000fieldValueA", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assert.assertEquals("datatype", key.getDataType());
        Assert.assertEquals("uid", key.getUid());
        Assert.assertEquals("fieldValueA", key.getFieldValue());
        Assert.assertEquals("fieldNameA", key.getFieldName());
        Assert.assertEquals(new Text("row"), key.getRow());
    }
    
    @Test
    public void parseShardDocKeyWithNull() {
        Key testKey = new Key("row", "datatype\u0000uid", "fieldNameA\u0000blah\u0000fieldValueA", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assert.assertEquals("datatype", key.getDataType());
        Assert.assertEquals("uid", key.getUid());
        Assert.assertEquals("blah\u0000fieldValueA", key.getFieldValue());
        Assert.assertEquals("fieldNameA", key.getFieldName());
        Assert.assertEquals(new Text("row"), key.getRow());
    }
    
    @Test
    public void parseShardFiKeyWithNull() {
        Key testKey = new Key("row", "fi\u0000fieldNameA", "fieldValueA\u0000blah\u0000datatype\u0000uid", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assert.assertEquals("datatype", key.getDataType());
        Assert.assertEquals("uid", key.getUid());
        Assert.assertEquals("fieldValueA\u0000blah", key.getFieldValue());
        Assert.assertEquals("fieldNameA", key.getFieldName());
        Assert.assertEquals(new Text("row"), key.getRow());
    }
    
    @Test
    public void parseShardFiKey() {
        Key testKey = new Key("row", "fi\u0000fieldNameA", "fieldValueA\u0000datatype\u0000uid", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assert.assertEquals("datatype", key.getDataType());
        Assert.assertEquals("uid", key.getUid());
        Assert.assertEquals("fieldValueA", key.getFieldValue());
        Assert.assertEquals("fieldNameA", key.getFieldName());
        Assert.assertEquals(new Text("row"), key.getRow());
    }
    
    @Test
    public void parseShardFiKeyInvalid() {
        Key testKey = new Key("row", "fi\u0000fieldNameA", "fieldValueA\u0000datatype", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assert.assertTrue(key.isInvalidKey());
    }
}
