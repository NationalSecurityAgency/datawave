package datawave.query.function.serializer;

import datawave.data.type.StringType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.JsonDeserializer;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;

public class SerializerDeterministicTest {


    private Document roundTrip(Document document) {

        JsonDocumentSerializer serializer = new JsonDocumentSerializer(false);

        JsonDeserializer deserializer = new JsonDeserializer();

        byte[] bytes = serializer.serialize(document);
        InputStream inputStream = new ByteArrayInputStream(bytes);

        Document output = deserializer.deserialize(inputStream);

        // compare
        System.err.println(document + " became "+output);
        return output;

    }

    @Test
    public void testContent() throws Exception {
        String field = "FOO";
        Content content = createContentType(field);
        Document document = new Document();
        document.put(field, content);

        Document out = roundTrip(document);
    }

    @Test
    public void testTypeAttribute() throws Exception {
        String field = "BAR";
        TypeAttribute<?> typeAttribute = createTypeAttribute(field);

        Document document = new Document();
        document.put(field, typeAttribute);
        Document out = roundTrip(document);
    }

    private Content createContentType(String field) {
        String uid = "parent.document.id";
        String datatype = "DATATYPE_A";
        String value = "foo";
        String cq = datatype + '\u0000' + uid + '\u0000' + value + '\u0000' + field;
        return new Content(value,
                new Key("shard", "datatype\0uid", cq, System.currentTimeMillis()), true);
    }

    private TypeAttribute<?> createTypeAttribute(String field) {
        String uid = "parent.document.id";
        String datatype = "DATATYPE_A";
        String value = "bar";
        String cq = datatype + '\u0000' + uid + '\u0000' + value + '\u0000' + field;
        StringType type = new StringType();
        type.setDelegate("A String");
        return new TypeAttribute<>(type,
                new Key("shard", "datatype\0uid", cq, System.currentTimeMillis()), true);
    }

    @Test
    public void testAttributes() throws Exception {

        Set<Attribute> attributes = new LinkedHashSet<>();
        attributes.add(createContentType("FOO"));
        attributes.add(createTypeAttribute("BAR"));


    }
}
