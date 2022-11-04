package datawave.query.data.parsers;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatawaveKeyTest {
    @Test
    public void parseShardDocKey() {
        Key testKey = new Key("row", "datatype\u0000uid", "fieldNameA\u0000fieldValueA", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assertions.assertEquals("datatype", key.getDataType());
        Assertions.assertEquals("uid", key.getUid());
        Assertions.assertEquals("fieldValueA", key.getFieldValue());
        Assertions.assertEquals("fieldNameA", key.getFieldName());
        Assertions.assertEquals(new Text("row"), key.getRow());
    }
    
    @Test
    public void parseShardDocKeyWithNull() {
        Key testKey = new Key("row", "datatype\u0000uid", "fieldNameA\u0000blah\u0000fieldValueA", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assertions.assertEquals("datatype", key.getDataType());
        Assertions.assertEquals("uid", key.getUid());
        Assertions.assertEquals("blah\u0000fieldValueA", key.getFieldValue());
        Assertions.assertEquals("fieldNameA", key.getFieldName());
        Assertions.assertEquals(new Text("row"), key.getRow());
    }
    
    @Test
    public void parseShardFiKeyWithNull() {
        Key testKey = new Key("row", "fi\u0000fieldNameA", "fieldValueA\u0000blah\u0000datatype\u0000uid", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assertions.assertEquals("datatype", key.getDataType());
        Assertions.assertEquals("uid", key.getUid());
        Assertions.assertEquals("fieldValueA\u0000blah", key.getFieldValue());
        Assertions.assertEquals("fieldNameA", key.getFieldName());
        Assertions.assertEquals(new Text("row"), key.getRow());
    }
    
    @Test
    public void parseShardFiKey() {
        Key testKey = new Key("row", "fi\u0000fieldNameA", "fieldValueA\u0000datatype\u0000uid", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        
        Assertions.assertEquals("datatype", key.getDataType());
        Assertions.assertEquals("uid", key.getUid());
        Assertions.assertEquals("fieldValueA", key.getFieldValue());
        Assertions.assertEquals("fieldNameA", key.getFieldName());
        Assertions.assertEquals(new Text("row"), key.getRow());
    }
    
    @Test
    public void parseShardFiKeyInvalid() {
        Key testKey = new Key("row", "fi\u0000fieldNameA", "fieldValueA\u0000datatype", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        Assertions.assertTrue(key.isInvalidKey());
    }
    
    @Test
    public void parseShardTfKey() {
        Key testKey = new Key("row", "tf", "datatype\u0000uid\u0000fieldValue\u0000fieldName", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        Assertions.assertFalse(key.isInvalidKey());
        Assertions.assertEquals("row", key.getShardId());
        Assertions.assertEquals(DatawaveKey.KeyType.TERM_OFFSETS, key.getType());
        Assertions.assertEquals("datatype", key.getDataType());
        Assertions.assertEquals("uid", key.getUid());
        Assertions.assertEquals("fieldValue", key.getFieldValue());
        Assertions.assertEquals("fieldName", key.getFieldName());
    }
    
    @Test
    public void parseShardTfKey_valueHasNulls() {
        Key testKey = new Key("row", "tf", "datatype\u0000uid\u0000fi\u0000eldVa\u0000lue\u0000fieldName", "viz");
        DatawaveKey key = new DatawaveKey(testKey);
        Assertions.assertEquals("row", key.getShardId());
        Assertions.assertEquals(DatawaveKey.KeyType.TERM_OFFSETS, key.getType());
        Assertions.assertEquals("datatype", key.getDataType());
        Assertions.assertEquals("uid", key.getUid());
        Assertions.assertEquals("fi\0eldVa\0lue", key.getFieldValue());
        Assertions.assertEquals("fieldName", key.getFieldName());
    }
}
