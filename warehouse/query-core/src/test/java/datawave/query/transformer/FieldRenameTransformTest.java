package datawave.query.transformer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;
import org.junit.Test;

import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;

public class FieldRenameTransformTest {

    @Test
    public void renameIdentityMapTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");

        Set<String> fieldMap = new HashSet<>();

        fieldMap.add("field2=field2");
        fieldMap.add("field1=field1");

        Document d = new Document();
        d.put("field1", new Numeric("1", key, true));
        d.put("field2", new Numeric("2", key, true));

        DocumentTransform transformer = new FieldRenameTransform(fieldMap, false, false);

        Map.Entry<Key,Document> transformed = transformer.apply(new UnmodifiableMapEntry(key, d));
        assertTrue(transformed.getValue() == d);

        assertTrue(d.containsKey("field2"));
        assertTrue(d.containsKey("field1"));
        assertEquals(2, d.getDictionary().size());
    }

    @Test
    public void renameFieldMapTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");

        Set<String> fieldMap = new HashSet<>();

        fieldMap.add("field2=field3");
        fieldMap.add("field1=field3");

        Document d = new Document();
        d.put("field1", new Numeric("1", key, true));
        d.put("field2", new Numeric("2", key, true));

        DocumentTransform transformer = new FieldRenameTransform(fieldMap, false, false);

        Map.Entry<Key,Document> transformed = transformer.apply(new UnmodifiableMapEntry(key, d));
        assertTrue(transformed.getValue() == d);

        assertTrue(d.containsKey("field3"));
        assertFalse(d.containsKey("field2"));
        assertFalse(d.containsKey("field1"));
        assertTrue(d.get("field3") instanceof Attributes);
        assertEquals(2, d.get("field3").size());
        assertEquals(1, d.getDictionary().size());
    }

    @Test
    public void renameFieldMapPreexistingTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");

        Set<String> fieldMap = new HashSet<>();

        fieldMap.add("field2=field3");
        fieldMap.add("field1=field3");

        Document d = new Document();
        d.put("field1", new Numeric("1", key, true));
        d.put("field2", new Numeric("2", key, true));
        d.put("field3", new Numeric("3", key, true));

        DocumentTransform transformer = new FieldRenameTransform(fieldMap, false, false);

        Map.Entry<Key,Document> transformed = transformer.apply(new UnmodifiableMapEntry(key, d));
        assertTrue(transformed.getValue() == d);

        assertTrue(d.containsKey("field3"));
        assertFalse(d.containsKey("field2"));
        assertFalse(d.containsKey("field1"));
        assertTrue(d.get("field3") instanceof Attributes);
        assertEquals(3, d.get("field3").size());
    }

    @Test
    public void renameWithGroupingContextAndMultipleMappings() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");

        Set<String> fieldMap = new HashSet<>();

        fieldMap.add("field2=field4");
        fieldMap.add("field1=field5");
        fieldMap.add("field1=field6");

        Document d = new Document();
        d.put("field1.field.11", new Numeric("1", key, true), true);
        d.put("field2.field.12", new Numeric("2", key, true), true);
        d.put("field3.field.13", new Numeric("3", key, true), true);

        DocumentTransform transformer = new FieldRenameTransform(fieldMap, true, false);

        Map.Entry<Key,Document> transformed = transformer.apply(new UnmodifiableMapEntry(key, d));
        assertTrue(transformed.getValue() == d);

        assertFalse(d.containsKey("field1.field.11"));
        assertFalse(d.containsKey("field2.field.12"));

        assertTrue(d.containsKey("field5.field.11"));
        assertTrue(d.containsKey("field6.field.11"));
        assertTrue(d.containsKey("field4.field.12"));
        assertTrue(d.containsKey("field3.field.13"));
        assertEquals(4, d.getDictionary().size());
    }
}
