package datawave.query.data.parsers;

import static datawave.query.data.parsers.DatawaveKey.KeyType.EVENT;
import static datawave.query.data.parsers.DatawaveKey.KeyType.INDEX_EVENT;
import static datawave.query.data.parsers.DatawaveKey.KeyType.TERM_OFFSETS;
import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import datawave.query.data.parsers.DatawaveKey.KeyType;

public class DatawaveKeyTest {

    private final String row = "row";
    private final String datatype = "datatype";
    private final String uid = "uid";
    private final String fieldName = "fieldName";
    private final String fieldValue = "fieldValue";
    private final String cv = "cv";

    private DatawaveKey parser;

    @Test
    public void testEventKey() {
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
    public void testEventKeyWithChildUid() {
        Key key = new Key(row, "datatype\u0000uid.1", "fieldName\u0000fieldValue", cv);
        parse(key);

        assertInvalid(false);
        assertType(EVENT);
        assertRow(row);
        assertDataType(datatype);
        assertUid("uid.1");
        assertFieldName(fieldName);
        assertFieldValue(fieldValue);
    }

    @Test
    public void testShardKeyWithExtraNulls() {
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
    public void testFiKeyWithExtraNulls() {
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
    public void testFiKey() {
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
    public void testFiKeyWithChildUid() {
        Key key = new Key(row, "fi\u0000fieldName", "fieldValue\u0000datatype\u0000uid.1", cv);
        parse(key);

        assertInvalid(false);
        assertType(INDEX_EVENT);
        assertRow(row);
        assertDataType(datatype);
        assertUid("uid.1");
        assertFieldName(fieldName);
        assertFieldValue(fieldValue);
    }

    @Test
    public void testInvalidFiKey() {
        Key key = new Key(row, "fi\u0000fieldName", "fieldValue\u0000datatype", cv);
        parse(key);
        assertInvalid(true);
    }

    @Test
    public void testTermFrequencyKey() {
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
    public void testTermFrequencyKeyWithChildUid() {
        Key key = new Key(row, "tf", "datatype\u0000uid.1\u0000fieldValue\u0000fieldName", cv);
        parse(key);

        assertInvalid(false);
        assertType(TERM_OFFSETS);
        assertRow(row);
        assertDataType(datatype);
        assertUid("uid.1");
        assertFieldName(fieldName);
        assertFieldValue(fieldValue);
    }

    @Test
    public void testTermFrequencyKeyWithExtraNulls() {
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

    @Test
    public void testGlobalIndexKey() {
        Key key = new Key("fieldValue", "fieldName", "row\0datatype");
        parse(key);

        assertInvalid(false);
        assertType(KeyType.INDEX);
        assertRow(fieldValue);
        assertDataType(datatype);
        assertUid(null);
        assertFieldName(fieldName);
        assertFieldValue(fieldValue);
    }

    @Test
    public void testGlobalIndexKeyWithNulls() {
        Key key = new Key("fie\0ldVa\0lue", "fieldName", "row\0datatype");
        parse(key);

        assertInvalid(false);
        assertType(KeyType.INDEX);
        assertRow("fie\0ldVa\0lue");
        assertDataType(datatype);
        assertUid(null);
        assertFieldName(fieldName);
        assertFieldValue("fie\0ldVa\0lue");
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
