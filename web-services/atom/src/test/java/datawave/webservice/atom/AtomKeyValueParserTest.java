package datawave.webservice.atom;

import org.apache.abdera.Abdera;
import org.apache.abdera.i18n.iri.IRI;
import org.apache.abdera.model.Entry;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class AtomKeyValueParserTest {
    
    public AtomKeyValueParser kv;
    
    @BeforeEach
    public void before() {
        kv = new AtomKeyValueParser();
    }
    
    @Test
    public void testGettersAndSetters() {
        kv.setValue("valueForTests");
        
        Assertions.assertNull(kv.getCollectionName());
        Assertions.assertNull(kv.getId());
        Assertions.assertNull(kv.getUpdated());
        Assertions.assertNull(kv.getColumnVisibility());
        Assertions.assertNull(kv.getUuid());
        Assertions.assertEquals("valueForTests", kv.getValue());
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testIDEncodeDecode() throws IOException {
        String id = "idForTests";
        String encodedID = kv.encodeId(id);
        String decodedID = kv.decodeId(encodedID);
        
        Assertions.assertNotEquals(id, encodedID);
        Assertions.assertNotEquals(decodedID, encodedID);
        Assertions.assertEquals(id, decodedID);
    }
    
    @Test
    public void testToEntry() {
        Abdera abdera = new Abdera();
        String host = "hostForTests";
        String port = "portForTests";
        
        IRI iri = new IRI("https://hostForTests:portForTests/DataWave/Atom/null/null");
        
        Entry entry = kv.toEntry(abdera, host, port);
        Assertions.assertEquals(iri, entry.getId());
        Assertions.assertEquals("(null) null with null @ null null", entry.getTitle());
        Assertions.assertNull(entry.getUpdated());
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testGoodParse() throws IOException {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        
        AtomKeyValueParser resultKV = kv.parse(key, value);
        
        Assertions.assertEquals("row1", resultKV.getCollectionName());
        Assertions.assertNotEquals(resultKV.getId(), kv.decodeId(resultKV.getId()));
        Assertions.assertEquals("color", resultKV.getUuid());
        Assertions.assertEquals("fi", resultKV.getValue());
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testMissingRowParts() {
        Key key = new Key(new Text("row1"), new Text("fi\0color"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> kv.parse(key, value));
        Assertions.assertEquals("Atom entry is missing row parts: row1 fi%00;color:red%00;truck%00;t-uid001 [] 9223372036854775807 false", e.getMessage());
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
        ValueFormatException e = Assertions.assertThrows(ValueFormatException.class, () -> kv.parse(key, value));
        Assertions.assertEquals("trying to convert to long, but byte array isn't long enough, wanted 8 found 0", e.getMessage());
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testMissingColQual() {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> kv.parse(key, value));
        Assertions.assertTrue(e.getMessage().startsWith("Atom entry is missing column qualifier parts: "));
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testBadDelimColQual() {
        Key key = new Key(new Text("row1\0row2and3"), new Text("fi\0"), new Text("red\0truck\0t-uid001"));
        byte[] vals = new byte[4];
        Value value = new Value(vals);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> kv.parse(key, value));
        Assertions.assertTrue(e.getMessage().startsWith("Atom entry is missing column qualifier parts: "));
    }
}
