package datawave.query.util.sortedmap.rfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.util.TypeMetadata;

public class KeyValueByteDocumenTransformsTest {

    protected final byte[] template = new byte[] {5, 2, 29, 4, 8, 3, 25, 23, 6, 21, 7, 16};

    @Test
    public void testDocumentTransforms() {
        List<Document> docs = createDocuments();
        for (Document d : docs) {
            Value v = KeyValueByteDocumentTransforms.documentToValue(d);
            Document d2 = KeyValueByteDocumentTransforms.valueToDocument(v);
            assertDocumentEquals(d, d2);
        }
    }

    @Test
    public void testByteTransforms() {
        List<byte[]> docs = createByteArrays();
        for (byte[] d : docs) {
            Key k = KeyValueByteDocumentTransforms.byteToKey(d);
            byte[] d2 = KeyValueByteDocumentTransforms.keyToByte(k);
            assertArrayEquals(d, d2);
        }
    }

    public List<byte[]> createByteArrays() {
        List<byte[]> docs = new ArrayList<>();
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 11];
            Arrays.fill(buffer, template[i]);
            docs.add(buffer);
        }
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 10];
            Arrays.fill(buffer, template[i]);
            docs.add(buffer);
        }
        return docs;
    }

    public List<Document> createDocuments() {
        List<Document> docs = new ArrayList<>();
        for (byte[] buffer : createByteArrays()) {
            docs.add(createDocument(buffer));
        }
        return docs;
    }

    public Document createDocument(byte[] values) {
        Key docKey = new Key("20200101_1", "datatype\u0000uid", "", values[0]);
        Key attrKey = new Key("20200101_1", "datatype\u0000uid", "FIELD\u0000VALUE", values[0]);
        List<Map.Entry<Key,Value>> attrs = new ArrayList<>();
        attrs.add(new UnmodifiableMapEntry(attrKey, new Value()));
        Document doc = new Document(docKey, Collections.singleton(docKey), false, attrs.iterator(),
                        new TypeMetadata().put("FIELD", "datatype", LcNoDiacriticsType.class.getName()), new CompositeMetadata(), true, true,
                        new EventDataQueryFieldFilter());
        return doc;
    }

    public static void assertDocumentEquals(Document d, Document d2) {
        // a document comparison that does not include comparing metadata as that does not survive the serialization process
        assertEquals(getDictionary(d), getDictionary(d2));
    }

    public static Multimap<String,String> getDictionary(Document d) {
        Multimap map = HashMultimap.create();
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> e : d.entrySet()) {
            String key = e.getKey();
            Attribute a = e.getValue();
            if (a instanceof Attributes) {
                for (Attribute a2 : ((Attributes) a).getAttributes()) {
                    map.put(key, String.valueOf(a2.getData()));
                }
            } else {
                map.put(key, String.valueOf(a.getData()));
            }
        }
        return map;
    }

}
