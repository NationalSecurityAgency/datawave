package datawave.query.attributes;

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.query.Constants;
import datawave.query.jexl.DatawaveJexlContext;

public class AttributeMetadataTest {
    private final String TF = "tf";
    private final String FI = "fi";
    private final String SHARD = "20240201_1";
    private final String DATATYPE = "datatype";
    private final String UID = datawave.data.hash.UID.builder().newId("FakeUID".getBytes(), (Date) null).toString();
    private final String FIELD_NAME = "name";
    private final String FIELD_VALUE = "value";
    private final String SEPARATOR = "\u0000";
    private final String VISIBILITY = "all";
    private final String VISIBILITY_SOME = "some";
    private final long NEW_TS = 100L;

    private final Key expectedMetadata = new Key(SHARD, DATATYPE + SEPARATOR + UID, "", VISIBILITY);
    private final Key expectedMetadataSomeVisibility = new Key(SHARD, DATATYPE + SEPARATOR + UID, "", VISIBILITY_SOME);
    private final Key expectedMetadataNewTS = new Key(SHARD, DATATYPE + SEPARATOR + UID, "", VISIBILITY, NEW_TS);
    private final Key expectedMetadataSomeVisibilityNewTS = new Key(SHARD, DATATYPE + SEPARATOR + UID, "", VISIBILITY_SOME, NEW_TS);
    private final Key emptyMetadata = new Key("", "", "", VISIBILITY);
    private final Key emptyMetadataSomeVisibility = new Key("", "", "", VISIBILITY_SOME, -1);
    private final Key emptyMetadataSomeVisibilityNewTS = new Key("", "", "", VISIBILITY_SOME, NEW_TS);
    private final ColumnVisibility expectedVisibility = new ColumnVisibility(VISIBILITY);
    private final ColumnVisibility someVisibility = new ColumnVisibility(VISIBILITY_SOME);

    @Test
    public void tfKeyTest() {
        Key key = new Key(SHARD, TF, DATATYPE + SEPARATOR + UID + SEPARATOR + FIELD_VALUE, VISIBILITY);
        TestAttribute attribute = new TestAttribute(key);
        assertEquals(expectedMetadata, attribute.getMetadata());
        assertEquals(expectedVisibility, attribute.getColumnVisibility());
    }

    @Test
    public void partialTfKeyTest() {
        Key key = new Key(SHARD, TF, DATATYPE + SEPARATOR + UID, VISIBILITY);
        TestAttribute attribute = new TestAttribute(key);
        assertEquals(expectedMetadata, attribute.getMetadata());
    }

    @Test
    public void fiKeyTest() {
        Key key = new Key(SHARD, FI + SEPARATOR + FIELD_NAME, FIELD_NAME + SEPARATOR + DATATYPE + SEPARATOR + UID, VISIBILITY);
        TestAttribute attribute = new TestAttribute(key);
        assertEquals(expectedMetadata, attribute.getMetadata());
        assertEquals(expectedVisibility, attribute.getColumnVisibility());
    }

    @Test
    public void eventKeyTest() {
        Key key = new Key(SHARD, DATATYPE + SEPARATOR + UID, FIELD_NAME + SEPARATOR + FIELD_NAME, VISIBILITY);
        TestAttribute attribute = new TestAttribute(key);
        assertEquals(expectedMetadata, attribute.getMetadata());
        assertEquals(expectedVisibility, attribute.getColumnVisibility());
    }

    @Test
    public void emptyAttributeTest() {
        TestAttribute attribute = new TestAttribute();
        assertEquals(null, attribute.getMetadata());
        assertEquals(Constants.EMPTY_VISIBILITY, attribute.getColumnVisibility());
        assertEquals(-1L, attribute.getTimestamp());
    }

    @Test
    public void setVisibilityTest() {
        Key key = new Key(SHARD, DATATYPE + SEPARATOR + UID, FIELD_NAME + SEPARATOR + FIELD_NAME, VISIBILITY);
        TestAttribute attribute = new TestAttribute(key);
        assertEquals(expectedVisibility, attribute.getColumnVisibility());

        attribute.setColumnVisibility(someVisibility);
        assertEquals(expectedMetadataSomeVisibility, attribute.getMetadata());
        assertEquals(someVisibility, attribute.getColumnVisibility());

        attribute = new TestAttribute();
        attribute.setColumnVisibility(someVisibility);
        assertEquals(emptyMetadataSomeVisibility, attribute.getMetadata());
        assertEquals(someVisibility, attribute.getColumnVisibility());

    }

    @Test
    public void setTimestampTest() {
        Key key = new Key(SHARD, DATATYPE + SEPARATOR + UID, FIELD_NAME + SEPARATOR + FIELD_NAME, VISIBILITY);
        TestAttribute attribute = new TestAttribute(key);
        assertEquals(expectedVisibility, attribute.getColumnVisibility());

        attribute.setTimestamp(NEW_TS);
        assertEquals(NEW_TS, attribute.getTimestamp());
        assertEquals(expectedMetadataNewTS, attribute.getMetadata());

        attribute = new TestAttribute();
        assertEquals(-1, attribute.getTimestamp());
        attribute.setTimestamp(NEW_TS);
        assertEquals(NEW_TS, attribute.getTimestamp());
    }

    @Test
    public void setMetadataVisibilityTSTest() {
        Key key = new Key(SHARD, DATATYPE + SEPARATOR + UID, FIELD_NAME + SEPARATOR + FIELD_NAME, VISIBILITY);
        TestAttribute attribute = new TestAttribute(key);
        assertEquals(expectedVisibility, attribute.getColumnVisibility());

        attribute.setMetadata(someVisibility, NEW_TS);
        assertEquals(expectedMetadataSomeVisibilityNewTS, attribute.getMetadata());
        assertEquals(someVisibility, attribute.getColumnVisibility());
        assertEquals(NEW_TS, attribute.getTimestamp());

        attribute = new TestAttribute();
        attribute.setMetadata(someVisibility, NEW_TS);
        assertEquals(emptyMetadataSomeVisibilityNewTS, attribute.getMetadata());
        assertEquals(someVisibility, attribute.getColumnVisibility());
        assertEquals(NEW_TS, attribute.getTimestamp());

    }

    private class TestAttribute extends Attribute<TestAttribute> {

        TestAttribute() {
            super();
        }

        TestAttribute(Key k) {
            super(k, true);
        }

        @Override
        public TestAttribute copy() {
            return null;
        }

        @Override
        public void write(DataOutput output, boolean reducedResponse) throws IOException {

        }

        @Override
        public void write(Kryo kryo, Output output, Boolean reducedResponse) {

        }

        @Override
        public Object getData() {
            return null;
        }

        @Override
        public Collection<ValueTuple> visit(Collection<String> fieldnames, DatawaveJexlContext context) {
            return null;
        }

        @Override
        public void write(Kryo kryo, Output output) {

        }

        @Override
        public void read(Kryo kryo, Input input) {

        }

        @Override
        public int compareTo(TestAttribute testAttribute) {
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }
}
