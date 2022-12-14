package datawave.query.data.parsers;

import datawave.query.data.parsers.DatawaveKey.KeyType;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static datawave.query.data.parsers.DatawaveKey.KeyType.EVENT;
import static datawave.query.data.parsers.DatawaveKey.KeyType.INDEX_EVENT;
import static datawave.query.data.parsers.DatawaveKey.KeyType.TERM_OFFSETS;
import static org.junit.Assert.assertEquals;

public class DatawaveKeyTest {
    
    private final String row = "row";
    private final String datatype = "datatype";
    private final String uid = "uid";
    private final String fieldName = "fieldName";
    private final String fieldValue = "fieldValue";
    private final String cv = "cv";
    
    private DatawaveKey parser;
    
    @Test
    public void parseShardDocKey() {
        Key key = new Key(row, "datatype\u0000uid", "fieldName\u0000fieldValue", cv);
        parse(key);
        
        assertInvalid(false);
        assertType(EVENT);
        assertRow(row);
        assertDataType(datatype);
        assertUid(uid);
        assertFieldName(fieldName);
        assertFieldValue(fieldValue);
    }
    
    @Test
    public void parseShardDocKeyWithNull() {
        Key key = new Key(row, "datatype\u0000uid", "fieldName\u0000blah\u0000fieldValue", cv);
        parse(key);
        
        assertInvalid(false);
        assertType(EVENT);
        assertRow(row);
        assertDataType(datatype);
        assertUid(uid);
        assertFieldName(fieldName);
        assertFieldValue("blah\u0000fieldValue");
    }
    
    @Test
    public void parseShardFiKeyWithNull() {
        Key key = new Key(row, "fi\u0000fieldName", "fieldValue\u0000blah\u0000datatype\u0000uid", cv);
        parse(key);
        
        assertInvalid(false);
        assertType(INDEX_EVENT);
        assertRow(row);
        assertDataType(datatype);
        assertUid(uid);
        assertFieldName(fieldName);
        assertFieldValue("fieldValue\u0000blah");
    }
    
    @Test
    public void parseShardFiKey() {
        Key key = new Key(row, "fi\u0000fieldName", "fieldValue\u0000datatype\u0000uid", cv);
        parse(key);
        
        assertInvalid(false);
        assertType(INDEX_EVENT);
        assertRow(row);
        assertDataType(datatype);
        assertUid(uid);
        assertFieldName(fieldName);
        assertFieldValue(fieldValue);
    }
    
    @Test
    public void parseShardFiKeyInvalid() {
        Key key = new Key(row, "fi\u0000fieldName", "fieldValue\u0000datatype", cv);
        parse(key);
        assertInvalid(true);
    }
    
    @Test
    public void parseShardTfKey() {
        Key key = new Key(row, "tf", "datatype\u0000uid\u0000fieldValue\u0000fieldName", cv);
        parse(key);
        
        assertInvalid(false);
        assertType(TERM_OFFSETS);
        assertRow(row);
        assertDataType(datatype);
        assertUid(uid);
        assertFieldName(fieldName);
        assertFieldValue(fieldValue);
    }
    
    @Test
    public void parseShardTfKey_valueHasNulls() {
        Key key = new Key(row, "tf", "datatype\u0000uid\u0000fi\u0000eldVa\u0000lue\u0000fieldName", cv);
        parse(key);
        
        assertInvalid(false);
        assertType(TERM_OFFSETS);
        assertRow(row);
        assertDataType(datatype);
        assertUid(uid);
        assertFieldName(fieldName);
        assertFieldValue("fi\0eldVa\0lue");
    }
    
    private void parse(Key key) {
        parser = new DatawaveKey(key);
    }
    
    private void assertShardId(String id) {
        assertEquals(id, parser.getShardId());
    }
    
    private void assertType(KeyType type) {
        assertEquals(type, parser.getType());
    }
    
    private void assertDataType(String dt) {
        assertEquals(dt, parser.getDataType());
    }
    
    private void assertUid(String uid) {
        assertEquals(uid, parser.getUid());
    }
    
    private void assertFieldName(String fn) {
        assertEquals(fn, parser.getFieldName());
    }
    
    private void assertFieldValue(String fv) {
        assertEquals(fv, parser.getFieldValue());
    }
    
    private void assertRow(String row) {
        assertEquals(new Text(row), parser.getRow());
    }
    
    private void assertInvalid(boolean invalid) {
        assertEquals(invalid, parser.isInvalidKey());
    }
}
