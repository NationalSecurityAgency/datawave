package datawave.query.function.serializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.DateType;
import datawave.data.type.GeoLatType;
import datawave.data.type.GeoLonType;
import datawave.data.type.GeoType;
import datawave.data.type.GeometryType;
import datawave.data.type.HexStringType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberListType;
import datawave.data.type.NumberType;
import datawave.data.type.PointType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.util.TypeMetadata;

/**
 * Evaluate performance of {@link KryoDocumentSerializer} and {@link KryoDocumentDeserializer}
 */
public abstract class KryoDocumentSerDeTest {

    protected final KryoDocumentSerializer serializer = new KryoDocumentSerializer();
    protected final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();

    private final String datatype = "datatype";
    private final Key documentKey = new Key("row", "datatype\0uid");

    protected Document d;
    private AttributeFactory attributeFactory;

    @BeforeEach
    public void setup() {
        TypeMetadata typeMetadata = getTypeMetadata();

        attributeFactory = new AttributeFactory(typeMetadata);

        d = new Document();
        d.put("DATE", createAttribute("DATE", String.valueOf(System.currentTimeMillis())));
        d.put("GEO_LAT", createAttribute("GEO_LAT", "-90"));
        d.put("GEO_LON", createAttribute("GEO_LON", "-180"));
        d.put("HEX", createAttribute("HEX", "a1b2c3"));
        d.put("IP", createAttribute("IP", "192.168.1.1"));
        d.put("IPV4", createAttribute("IPV4", "192.168.1.1"));
        d.put("LC", createAttribute("LC", "value-1"));
        d.put("NC_ND", createAttribute("NC_ND", "value-2"));
        d.put("NC_ND_LIST", createAttribute("NC_ND", "value-2"));
        d.put("NUM", createAttribute("NUM", "25"));
        d.put("NUM_LIST", createAttribute("NUM_LIST", "22,23,24"));
        d.put("POINT", createAttribute("POINT", "POINT(10 10)"));
    }

    private TypeMetadata getTypeMetadata() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("DATE", datatype, DateType.class.getTypeName());
        typeMetadata.put("GEO_LAT", datatype, GeoLatType.class.getTypeName());
        typeMetadata.put("GEO_LON", datatype, GeoLonType.class.getTypeName());
        typeMetadata.put("GEOMETRY", datatype, GeometryType.class.getTypeName());
        typeMetadata.put("GEO", datatype, GeoType.class.getTypeName());
        typeMetadata.put("HEX", datatype, HexStringType.class.getTypeName());
        typeMetadata.put("LC", datatype, LcType.class.getTypeName());
        typeMetadata.put("LC_ND", datatype, LcNoDiacriticsType.class.getTypeName());
        typeMetadata.put("NO_OP", datatype, NoOpType.class.getTypeName());
        typeMetadata.put("NUM", datatype, NumberType.class.getTypeName());
        typeMetadata.put("NUM_LIST", datatype, NumberListType.class.getTypeName());
        typeMetadata.put("POINT", datatype, PointType.class.getTypeName());
        return typeMetadata;
    }

    protected Attribute<?> createAttribute(String field, String value) {
        return attributeFactory.create(field, value, documentKey, datatype, true);
    }

    static class KryoDocumentSerializerTest extends KryoDocumentSerDeTest {
        @Test
        public void testBulkSerialization() {
            int max = 1_000_000;
            for (int i = 1; i <= max; i++) {
                byte[] bytes = serializer.serialize(d);
                assertTrue(450 < bytes.length && bytes.length <= 460);
            }
        }
    }

    static class KryoDocumentDeserializerTest extends KryoDocumentSerDeTest {
        @Test
        public void testBulkDeserialization() {
            byte[] data = serializer.serialize(d);

            int max = 1_000_000;
            for (int i = 1; i <= max; i++) {
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                Document d = deserializer.deserialize(bais);
                assertEquals(12, d.size());
            }
        }
    }

}
