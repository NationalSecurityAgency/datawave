package datawave.marking;

import static datawave.marking.AccessExpressionMarkings.ACCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.InvalidAccessExpressionException;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

public class AccessExpressionMarkingsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Schema<AccessExpressionMarkings> SCHEMA = AccessExpressionMarkings.SCHEMA;

    // -- getMarkings / toAccessExpression --

    @Test
    public void testGetMarkingsReturnsAccessExpression() {
        AccessExpression ae = ACCESS.newExpression("A&B");
        AccessExpressionMarkings markings = AccessExpressionMarkings.builder().accessExpression(ae).build();
        assertEquals(ae, markings.getMarkings());
        assertEquals(ae, markings.toAccessExpression());
    }

    // -- toColumnVisibility --

    @Test
    public void testToColumnVisibility() {
        AccessExpressionMarkings markings = AccessExpressionMarkings.create("B&A");
        ColumnVisibility cv = markings.toColumnVisibility();
        // normalized: sorted
        assertEquals("A&B", new String(cv.getExpression()));
    }

    // -- isEmpty --

    @Test
    public void testIsEmptyWithNullExpression() {
        AccessExpressionMarkings markings = AccessExpressionMarkings.builder().accessExpression(null).build();
        assertTrue(markings.isEmpty());
    }

    @Test
    public void testIsEmptyWithEmptyExpression() {
        assertNull(AccessExpressionMarkings.create(""));
    }

    @Test
    public void testIsEmptyWithNonEmptyExpression() {
        AccessExpressionMarkings markings = AccessExpressionMarkings.create("A");
        assertFalse(markings.isEmpty());
    }

    // -- getColumnVisibilityString --

    @Test
    public void testGetColumnVisibilityString() {
        AccessExpressionMarkings markings = AccessExpressionMarkings.create("X|Y");
        assertEquals("X|Y", markings.getColumnVisibilityString());
    }

    @Test
    public void testGetColumnVisibilityStringNull() {
        AccessExpressionMarkings markings = AccessExpressionMarkings.builder().accessExpression(null).build();
        assertNull(markings.getColumnVisibilityString());
    }

    // -- create (JsonCreator) --

    @Test
    public void testCreateWithNull() {
        assertNull(AccessExpressionMarkings.create(null));
    }

    @Test
    public void testCreateWithExpression() {
        AccessExpressionMarkings markings = AccessExpressionMarkings.create("A&B&C");
        assertEquals("A&B&C", markings.getColumnVisibilityString());
    }

    @Test
    public void testCreateIllegalOperator1() {
        assertThrows(InvalidAccessExpressionException.class, () -> AccessExpressionMarkings.create("A&&B&C"));
    }

    @Test
    public void testCreateIllegalOperator2() {
        assertThrows(InvalidAccessExpressionException.class, () -> AccessExpressionMarkings.create("A||B&C"));
    }

    @Test
    public void testCreateMalformed1() {
        assertThrows(InvalidAccessExpressionException.class, () -> AccessExpressionMarkings.create("(A&B&C))"));
    }

    @Test
    public void testCreateMalformed2() {
        assertThrows(InvalidAccessExpressionException.class, () -> AccessExpressionMarkings.create("(A&B,C))"));
    }

    // -- createMarkings(ColumnVisibility) --

    @Test
    public void testCreateMarkingsFromColumnVisibilityNull() {
        assertNull(AccessExpressionMarkings.createMarkings((ColumnVisibility) null));
    }

    @Test
    public void testCreateMarkingsFromColumnVisibility() {
        ColumnVisibility cv = new ColumnVisibility("A|B");
        AccessExpressionMarkings markings = AccessExpressionMarkings.createMarkings(cv);
        assertEquals("A|B", markings.getColumnVisibilityString());
    }

    // -- createMarkings(AccessExpression) --

    @Test
    public void testCreateMarkingsFromAccessExpressionNull() {
        assertNull(AccessExpressionMarkings.createMarkings((AccessExpression) null));
    }

    @Test
    public void testCreateMarkingsFromAccessExpression() {
        AccessExpression ae = ACCESS.newExpression("C&D");
        AccessExpressionMarkings markings = AccessExpressionMarkings.createMarkings(ae);
        assertEquals("C&D", markings.getColumnVisibilityString());
    }

    // -- createMarkings(Text) --

    @Test
    public void testCreateMarkingsFromTextNull() {
        assertNull(AccessExpressionMarkings.createMarkings((Text) null));
    }

    @Test
    public void testCreateMarkingsFromText() {
        AccessExpressionMarkings markings = AccessExpressionMarkings.createMarkings(new Text("E|F"));
        assertEquals("E|F", markings.getColumnVisibilityString());
    }

    // -- createMarkings(String) --

    @Test
    public void testCreateMarkingsFromStringNull() {
        assertNull(AccessExpressionMarkings.createMarkings((String) null));
    }

    @Test
    public void testCreateMarkingsFromString() {
        AccessExpressionMarkings markings = AccessExpressionMarkings.createMarkings("G&H");
        assertEquals("G&H", markings.getColumnVisibilityString());
    }

    // -- JSON round-trip --

    @Test
    public void testJsonSerialization() throws Exception {
        AccessExpressionMarkings original = AccessExpressionMarkings.create("A&B");
        String json = MAPPER.writeValueAsString(original);
        assertTrue(json.contains("\"columnVisibility\":\"A&B\""));
    }

    @Test
    public void testJsonDeserialization() throws Exception {
        String json = "{\"@class\":\"datawave.marking.AccessExpressionMarkings\",\"columnVisibility\":\"X|Y\"}";
        AccessExpressionMarkings markings = MAPPER.readValue(json, AccessExpressionMarkings.class);
        assertEquals("X|Y", markings.getColumnVisibilityString());
    }

    @Test
    public void testJsonRoundTrip() throws Exception {
        AccessExpressionMarkings original = AccessExpressionMarkings.create("A&B&C");
        String json = MAPPER.writeValueAsString(original);
        // round-trip through the polymorphic Markings type
        Markings<?> deserialized = MAPPER.readValue(json, Markings.class);
        assertInstanceOf(AccessExpressionMarkings.class, deserialized);
        assertEquals(original.getColumnVisibilityString(), ((AccessExpressionMarkings) deserialized).getColumnVisibilityString());
    }

    // -- equals via Lombok @Data --

    @Test
    public void testEquality() {
        AccessExpressionMarkings a = AccessExpressionMarkings.create("A&B");
        AccessExpressionMarkings b = AccessExpressionMarkings.create("A&B");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    // -- JAXB round-trip --

    @Test
    public void testJaxbMarshal() throws Exception {
        AccessExpressionMarkings original = AccessExpressionMarkings.create("A&B");
        String xml = makeJaxbMarshall(original);
        assertTrue(xml.contains("<columnVisibility>A&amp;B</columnVisibility>"), "XML should contain the columnVisibility element: " + xml);
    }

    @Test
    public void testJaxbUnmarshal() throws Exception {
        String expression = "X|Y";
        AccessExpressionMarkings markings = makeJaxbUnmarshall(expression);
        assertEquals("X|Y", markings.getColumnVisibilityString());
        assertFalse(markings.isEmpty());
    }

    @Test
    public void testJaxbRoundTrip() throws Exception {
        AccessExpressionMarkings original = AccessExpressionMarkings.create("(A&B)|C");
        AccessExpressionMarkings result = makeJaxbRoundTrip(original);
        assertEquals(original.getColumnVisibilityString(), result.getColumnVisibilityString());
    }

    @Test
    public void testJaxbRoundTripEmpty() throws Exception {
        AccessExpressionMarkings original = AccessExpressionMarkings.builder().build();
        AccessExpressionMarkings result = makeJaxbRoundTrip(original);

        assertNull(result.getColumnVisibilityString());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testJaxbRoundTripComplexExpression() throws Exception {
        AccessExpressionMarkings original = AccessExpressionMarkings.create("(A&B)|(C&D&E)");
        AccessExpressionMarkings result = makeJaxbRoundTrip(original);

        assertEquals("(A&B)|(C&D&E)", result.getColumnVisibilityString());
        assertFalse(result.isEmpty());
    }
    // -- Protostuff Schema metadata --

    @Test
    public void testSchemaGetFieldNameKnown() {
        assertEquals(AccessExpressionMarkings.COLUMN_VISIBILITY_KEY, SCHEMA.getFieldName(1));
    }

    @Test
    public void testSchemaGetFieldNameUnknown() {
        assertNull(SCHEMA.getFieldName(0));
        assertNull(SCHEMA.getFieldName(2));
    }

    @Test
    public void testSchemaGetFieldNumberKnown() {
        assertEquals(1, SCHEMA.getFieldNumber(AccessExpressionMarkings.COLUMN_VISIBILITY_KEY));
    }

    @Test
    public void testSchemaGetFieldNumberUnknown() {
        assertEquals(0, SCHEMA.getFieldNumber("nonExistentField"));
    }

    @Test
    public void testSchemaIsInitialized() {
        assertTrue(SCHEMA.isInitialized(AccessExpressionMarkings.create("A")));
        assertTrue(SCHEMA.isInitialized(AccessExpressionMarkings.builder().build()));
    }

    @Test
    public void testSchemaNewMessage() {
        AccessExpressionMarkings msg = SCHEMA.newMessage();
        assertNotNull(msg);
        assertTrue(msg.isEmpty());
    }

    @Test
    public void testSchemaMessageName() {
        assertEquals("AccessExpressionMarkings", SCHEMA.messageName());
    }

    @Test
    public void testSchemaMessageFullName() {
        assertEquals("datawave.marking.AccessExpressionMarkings", SCHEMA.messageFullName());
    }

    @Test
    public void testSchemaTypeClass() {
        assertEquals(AccessExpressionMarkings.class, SCHEMA.typeClass());
    }

    // -- Protostuff Schema round-trip serialization --

    @Test
    public void testProtostuffRoundTrip() {
        AccessExpressionMarkings original = AccessExpressionMarkings.create("(A&B)|C");
        AccessExpressionMarkings deserialized = makeProtostuffRoundTrip(original);
        assertEquals(original.getColumnVisibilityString(), deserialized.getColumnVisibilityString());
    }

    @Test
    public void testProtostuffRoundTripEmpty() {
        AccessExpressionMarkings original = AccessExpressionMarkings.builder().build();
        AccessExpressionMarkings deserialized = makeProtostuffRoundTrip(original);
        assertTrue(deserialized.isEmpty());
    }

    @Test
    public void testProtostuffRoundTripComplexExpression() {
        AccessExpressionMarkings original = AccessExpressionMarkings.create("(A&B)|(C&D&E)");
        AccessExpressionMarkings deserialized = makeProtostuffRoundTrip(original);
        assertEquals("(A&B)|(C&D&E)", deserialized.getColumnVisibilityString());
        assertFalse(deserialized.isEmpty());
    }

    private static String makeJaxbMarshall(AccessExpressionMarkings original) throws JAXBException {
        JAXBContext ctx = JAXBContext.newInstance(AccessExpressionMarkings.class);
        ByteArrayOutputStream out = marshal(original, ctx);
        return out.toString();
    }

    private static AccessExpressionMarkings makeJaxbUnmarshall(String expression) throws JAXBException, IOException {
        JAXBContext ctx = JAXBContext.newInstance(AccessExpressionMarkings.class);

        String xml = String.format(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                                        + "<markings xmlns=\"http://webservice.datawave.nsa/v1\"><columnVisibility>%s</columnVisibility></markings>",
                        expression);

        JAXBElement<AccessExpressionMarkings> element;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            out.write(xml.getBytes());
            element = unmarshal(out, ctx);
        }
        return element.getValue();
    }

    private static AccessExpressionMarkings makeJaxbRoundTrip(AccessExpressionMarkings original) throws JAXBException {
        JAXBContext ctx = JAXBContext.newInstance(AccessExpressionMarkings.class);
        ByteArrayOutputStream out = marshal(original, ctx);
        JAXBElement<AccessExpressionMarkings> result = unmarshal(out, ctx);
        return result.getValue();
    }

    private static ByteArrayOutputStream marshal(AccessExpressionMarkings original, JAXBContext ctx) throws JAXBException {
        Marshaller marshaller = ctx.createMarshaller();
        JAXBElement<AccessExpressionMarkings> element = new JAXBElement<>(new QName("http://webservice.datawave.nsa/v1", "markings"),
                        AccessExpressionMarkings.class, original);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        marshaller.marshal(element, out);
        return out;
    }

    private static JAXBElement<AccessExpressionMarkings> unmarshal(ByteArrayOutputStream out, JAXBContext ctx) throws JAXBException {
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        return unmarshaller.unmarshal(new StreamSource(new ByteArrayInputStream(out.toByteArray())), AccessExpressionMarkings.class);
    }

    private static AccessExpressionMarkings makeProtostuffRoundTrip(AccessExpressionMarkings original) {
        LinkedBuffer buffer = LinkedBuffer.allocate();
        byte[] bytes = ProtostuffIOUtil.toByteArray(original, SCHEMA, buffer);

        AccessExpressionMarkings deserialized = SCHEMA.newMessage();
        ProtostuffIOUtil.mergeFrom(bytes, deserialized, SCHEMA);
        return deserialized;
    }

}
