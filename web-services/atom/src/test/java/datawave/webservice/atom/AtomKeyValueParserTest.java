package datawave.webservice.atom;

import java.io.IOException;

import org.apache.abdera.Abdera;
import org.apache.abdera.i18n.iri.IRI;
import org.apache.abdera.model.Entry;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AtomKeyValueParserTest {

    public AtomKeyValueParser kv;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        kv = new AtomKeyValueParser();
    }

    @Test
    public void testGettersAndSetters() {
        kv.setValue("valueForTests");

        Assert.assertNull(kv.getCollectionName());
        Assert.assertNull(kv.getId());
        Assert.assertNull(kv.getUpdated());
        Assert.assertNull(kv.getColumnVisibility());
        Assert.assertNull(kv.getUuid());
        Assert.assertEquals("valueForTests", kv.getValue());
    }

    @SuppressWarnings("static-access")
    @Test
    public void testIDEncodeDecode() throws IOException {
        String id = "idForTests";
        String encodedID = kv.encodeId(id);
        String decodedID = kv.decodeId(encodedID);

        Assert.assertNotEquals(id, encodedID);
        Assert.assertNotEquals(decodedID, encodedID);
        Assert.assertEquals(id, decodedID);
    }

    @Test
    public void testToEntry() {
        Abdera abdera = new Abdera();
        String host = "hostForTests";
        String port = "portForTests";

        IRI iri = new IRI("https://hostForTests:portForTests/DataWave/Atom/null/null");

        Entry entry = kv.toEntry(abdera, host, port);
        Assert.assertEquals(iri, entry.getId());
        Assert.assertEquals("(null) null with null @ null null", entry.getTitle());
        Assert.assertNull(entry.getUpdated());
    }

    @SuppressWarnings("static-access")
    @Test
    public void testGoodParse() throws IOException {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);

        AtomKeyValueParser resultKV = kv.parse(key, value);

        Assert.assertEquals("row1", resultKV.getCollectionName());
        Assert.assertNotEquals(resultKV.getId(), kv.decodeId(resultKV.getId()));
        Assert.assertEquals("color", resultKV.getUuid());
        Assert.assertEquals("fi", resultKV.getValue());
    }

    @SuppressWarnings("static-access")
    @Test
    public void testMissingRowParts() {
        Key key = new Key(new Text("row1"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Atom entry is missing row parts: row1 fi%00;color:red%00;truck%00;t-uid001 [] 9223372036854775807 false");
        try {
            kv.parse(key, value);
        } catch (IOException e) {
            // Empty on purpose
        }
    }

    @SuppressWarnings("static-access")
    @Test
    public void testTooManyRowParts() {
        Key key = new Key(new Text("row1\0row2and3\0row4"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        try {
            kv.parse(key, value);
        } catch (IOException e) {
            // Empty on purpose
        }
    }

    @SuppressWarnings("static-access")
    @Test
    public void testDelimiterAtEnd() {
        Key key = new Key(new Text("row1\0"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        thrown.expect(ValueFormatException.class);
        thrown.expectMessage("trying to convert to long, but byte array isn't long enough, wanted 8 found 0");
        try {
            kv.parse(key, value);
        } catch (IOException e) {
            // Empty on purpose
        }
    }

    @SuppressWarnings("static-access")
    @Test
    public void testMissingColQual() {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Atom entry is missing column qualifier parts: ");
        try {
            kv.parse(key, value);
        } catch (IOException e) {
            // Empty catch because of ExpectedException
        }
    }

    @SuppressWarnings("static-access")
    @Test
    public void testBadDelimColQual() {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi\0"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Atom entry is missing column qualifier parts: ");
        try {
            kv.parse(key, value);
        } catch (IOException e) {
            // Empty catch because of ExpectedException
        }
    }
}
