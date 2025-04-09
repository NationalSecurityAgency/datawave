package datawave.query.attributes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.AbstractMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.type.HexStringType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NumberType;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.util.TypeMetadata;

/**
 * Test document serialization with various attributes
 */
public class DocumentTest {

    private static final Logger log = LoggerFactory.getLogger(DocumentTest.class);

    private final String datatype = "datatype";
    private final Key documentKey = new Key("row", "datatype\0uid");
    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();
    private final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();

    private static final int max_iterations = 1;

    private AttributeFactory attributeFactory;

    private Document d;

    @BeforeEach
    public void setup() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("LC", datatype, LcType.class.getTypeName());
        typeMetadata.put("LC_NO_DIACRITICS", datatype, LcNoDiacriticsType.class.getTypeName());
        typeMetadata.put("NUMBER", datatype, NumberType.class.getTypeName());
        typeMetadata.put("HEX", datatype, HexStringType.class.getTypeName());

        attributeFactory = new AttributeFactory(typeMetadata);
        d = new Document(documentKey, true);
    }

    @Test
    public void testEmptyDocument() {
        roundTrip(max_iterations);
    }

    @Test
    public void testDocumentWithLcType() {
        Attribute<?> attr = createAttribute("LC", "value");
        d.put("LC", attr);
        roundTrip(max_iterations);
    }

    @Test
    public void testDocumentWithLcNoDiacriticsType() {
        Attribute<?> attr = createAttribute("LC_NO_DIACRITICS", "value");
        d.put("LC_NO_DIACRITICS", attr);
        roundTrip(max_iterations);
    }

    @Test
    public void testDocumentWithHexType() {
        Attribute<?> attr = createAttribute("HEX", "a1b2c3");
        d.put("HEX", attr);
        roundTrip(max_iterations);
    }

    @Test
    public void testDocumentWithNumberType() {
        Attribute<?> attr = createAttribute("NUMBER", "12");
        d.put("NUMBER", attr);
        roundTrip(max_iterations);
    }

    @Test
    public void testDocumentWithNumberTypeNormalizedValue() {
        Attribute<?> attr = createAttribute("NUMBER", "+bE1.2");
        d.put("NUMBER", attr);
        roundTrip(max_iterations);
    }

    @Test
    public void testDocumentWithNumberTypeLargeValue() {
        Attribute<?> attr = createAttribute("NUMBER", "12456789.987654321");
        d.put("NUMBER", attr);
        roundTrip(max_iterations);
    }

    @Test
    public void testOneOfEverything() {
        Attribute<?> attr1 = createAttribute("LC", "value-1");
        Attribute<?> attr2 = createAttribute("LC_NO_DIACRITICS", "value-2");
        Attribute<?> attr3 = createAttribute("NUMBER", "25");
        Attribute<?> attr4 = createAttribute("HEX", "a1b2c3");
        d.put("LC", attr1);
        d.put("LC_NO_DIACRITICS", attr2);
        d.put("NUMBER", attr3);
        d.put("HEX", attr4);
        roundTrip(max_iterations);
    }

    @Test
    public void testLargeDocument() {
        int size = 100_000;
        for (int i = 0; i < size; i++) {
            Attribute<?> attr = createAttribute("LC", "value-" + i);
            d.put("LC", attr);
        }
        roundTrip(max_iterations);
    }

    protected void roundTrip(int max) {
        Entry<Key,Value> entry = serialize(d);
        log.trace("size: {}", entry.getValue().getSize());
        for (int i = 0; i < max; i++) {
            Entry<Key,Document> result = deserialize(entry);
            Document d2 = result.getValue();
            assertEquals(d, d2);
        }
    }

    protected Attribute<?> createAttribute(String field, String value) {
        Attribute<?> attr = attributeFactory.create(field, value, documentKey, datatype, true);
        attr.clearMetadata();
        return attr;
    }

    protected Entry<Key,Value> serialize(Document d) {
        Entry<Key,Document> entry = new AbstractMap.SimpleEntry<>(documentKey, d);
        return serializer.apply(entry);
    }

    protected Entry<Key,Document> deserialize(Entry<Key,Value> entry) {
        return deserializer.apply(entry);
    }
}
