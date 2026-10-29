package datawave.webservice.atom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.abdera.Abdera;
import org.apache.abdera.i18n.iri.IRI;
import org.apache.abdera.model.Entry;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AtomKeyValueParserTest {

    public AtomKeyValueParser kv;

    @BeforeEach
    public void before() {
        kv = new AtomKeyValueParser();
    }

    @Test
    public void testGettersAndSetters() {
        kv.setValue("valueForTests");

        assertNull(kv.getCollectionName());
        assertNull(kv.getId());
        assertNull(kv.getUpdated());
        assertNull(kv.getColumnVisibility());
        assertNull(kv.getUuid());
        assertEquals("valueForTests", kv.getValue());
    }

    @SuppressWarnings("static-access")
    @Test
    public void testIDEncodeDecode() throws IOException {
        String id = "idForTests";
        String encodedID = AtomKeyValueParser.encodeId(id);
        String decodedID = AtomKeyValueParser.decodeId(encodedID);

        assertNotEquals(id, encodedID);
        assertNotEquals(decodedID, encodedID);
        assertEquals(id, decodedID);
    }

    @Test
    public void testToEntry() {
        Abdera abdera = new Abdera();
        String host = "hostForTests";
        String port = "portForTests";

        IRI iri = new IRI("https://hostForTests:portForTests/DataWave/Atom/null/null");

        Entry entry = kv.toEntry(abdera, host, port);
        assertEquals(iri, entry.getId());
        assertEquals("(null) null with null @ null null", entry.getTitle());
        assertNull(entry.getUpdated());
    }

    @SuppressWarnings("static-access")
    @Test
    public void testGoodParse() throws IOException {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);

        AtomKeyValueParser resultKV = AtomKeyValueParser.parse(key, value);

        assertEquals("row1", resultKV.getCollectionName());
        assertNotEquals(resultKV.getId(), AtomKeyValueParser.decodeId(resultKV.getId()));
        assertEquals("color", resultKV.getUuid());
        assertEquals("fi", resultKV.getValue());
    }

    @SuppressWarnings("static-access")
    @Test
    public void testMissingRowParts() {
        Key key = new Key(new Text("row1"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        Exception expectedEx = assertThrows(IllegalArgumentException.class, () -> AtomKeyValueParser.parse(key, value));
        assertTrue(expectedEx.getMessage().equals("Atom entry is missing row parts: row1 fi%00;color:red%00;truck%00;t-uid001 [] 9223372036854775807 false"));
    }

    @SuppressWarnings("static-access")
    @Test
    public void testTooManyRowParts() {
        Key key = new Key(new Text("row1\0row2and3\0row4"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        try {
            AtomKeyValueParser.parse(key, value);
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
        Exception expectedEx = assertThrows(ValueFormatException.class, () -> AtomKeyValueParser.parse(key, value));
        assertTrue(expectedEx.getMessage().equals("trying to convert to long, but byte array isn't long enough, wanted 8 found 0"));
    }

    @SuppressWarnings("static-access")
    @Test
    public void testMissingColQual() {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        Exception expectedEx = assertThrows(IllegalArgumentException.class, () -> AtomKeyValueParser.parse(key, value));
        assertTrue(expectedEx.getMessage().contains("Atom entry is missing column qualifier parts: "));
    }

    @SuppressWarnings("static-access")
    @Test
    public void testBadDelimColQual() {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi\0"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        Exception excpectedEx = assertThrows(IllegalArgumentException.class, () -> AtomKeyValueParser.parse(key, value));
        assertTrue(excpectedEx.getMessage().contains("Atom entry is missing column qualifier parts: "));
    }
}
