package datawave.query.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.util.TypeMetadata;

public class RemoveGroupingContextTest {
    @Test
    public void cleanupDocumentTest() {
        Map<Key,Value> map = new HashMap<>();
        map.put(new Key("20250512_3", "simpleType\u00001.2.3", "foo.1.2.3\u0000a"), new Value());
        map.put(new Key("20250512_3", "simpleType\u00001.2.3", "bar.1.2.3\u0000b"), new Value());
        map.put(new Key("20250512_3", "simpleType\u00001.2.3", "foo.1.2.2\u0000c"), new Value());
        map.put(new Key("20250512_3", "simpleType\u00001.2.3", "bar.1.2.2\u0000d"), new Value());
        map.put(new Key("20250512_3", "simpleType\u00001.2.3", "ungrouped\u0000e"), new Value());
        map.put(new Key("20250512_3", "simpleType\u00001.2.3", "foo\u0000z"), new Value());
        TypeMetadata typeMetadata = new TypeMetadata();
        CompositeMetadata compositeMetadata = new CompositeMetadata();
        Document d = new Document();
        d.consumeRawData(new Key("20250512_3"), Set.of(new Key()), map.entrySet().iterator(), typeMetadata, compositeMetadata, true, true, null, false);

        RemoveGroupingContext removeGroupingContext = new RemoveGroupingContext();
        removeGroupingContext.apply(new AbstractMap.SimpleEntry<>(new Key(), d));

        // fields + RECORD_ID
        assertEquals(4, d.getDictionary().size());

        assertNotNull(d.getDictionary().get("foo"));
        assertNotNull(d.getDictionary().get("bar"));
        assertNotNull(d.getDictionary().get("ungrouped"));
        assertNotNull(d.getDictionary().get("RECORD_ID"));

        assertNull(d.getDictionary().get("foo.1.2.3"));
        assertNull(d.getDictionary().get("bar.1.2.3"));
        assertNull(d.getDictionary().get("foo.1.2.2"));
        assertNull(d.getDictionary().get("bar.1.2.2"));

        Attribute<? extends Comparable<?>> foo = d.getDictionary().get("foo");
        assertTrue(foo instanceof Attributes);
        Attributes fooAttributes = (Attributes) foo;
        assertEquals(3, fooAttributes.size());
        List<String> expectedFooData = new ArrayList<>(List.of("a", "c", "z"));
        for (Attribute<? extends Comparable<?>> attr : fooAttributes.getAttributes()) {
            expectedFooData.remove(attr.getData().toString());
        }
        assertEquals(0, expectedFooData.size());
        Attribute<? extends Comparable<?>> bar = d.getDictionary().get("bar");
        assertTrue(bar instanceof Attributes);
        Attributes barAttributes = (Attributes) bar;
        assertEquals(2, barAttributes.size());
        List<String> expectedBarData = new ArrayList<>(List.of("b", "d"));
        for (Attribute<? extends Comparable<?>> attr : barAttributes.getAttributes()) {
            expectedBarData.remove(attr.getData().toString());
        }
        assertEquals(0, expectedBarData.size());

        assertEquals("e", d.getDictionary().get("ungrouped").getData().toString());
    }
}
