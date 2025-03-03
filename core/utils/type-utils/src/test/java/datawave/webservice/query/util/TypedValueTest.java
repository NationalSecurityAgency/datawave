package datawave.webservice.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.xml.bind.JAXBContext;

import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.BaseType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;

public class TypedValueTest {
    
    private static String EXPECTED_FORMAT;
    private static String EXPECTED_BASE64_FORMAT;
    private JAXBContext ctx;
    
    @BeforeAll
    public static void setUpClass() throws Exception {
        System.setProperty("user.timezone", "GMT");
        BufferedReader rdr;
        rdr = new BufferedReader(new InputStreamReader(TypedValueTest.class.getResourceAsStream("TypedValueExpectedUnencoded.xml")));
        EXPECTED_FORMAT = rdr.readLine();
        rdr.close();
        rdr = new BufferedReader(new InputStreamReader(TypedValueTest.class.getResourceAsStream("TypedValueExpectedEncoded.xml")));
        EXPECTED_BASE64_FORMAT = rdr.readLine();
        rdr.close();
    }
    
    @BeforeEach
    public void setUp() throws Exception {
        ctx = JAXBContext.newInstance(TypedValue.class);
    }
    
    @Test
    public void testPlainString() throws Exception {
        TypedValue value = new TypedValue("plainString");
        String actual = serialize(value);
        assertEquals(expected("xs:string", "plainString"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:string", value.getType());
    }
    
    @Test
    public void testEncodedStringString() throws Exception {
        TypedValue value = new TypedValue("encoded\0String");
        String actual = serialize(value);
        assertEquals(expected64("xs:string", new String(Base64.encodeBase64("encoded\0String".getBytes()))), actual);
        assertTrue(value.isBase64Encoded());
        assertEquals("xs:string", value.getType());
    }
    
    @Test
    public void testEncodedNoOpTypeString() throws Exception {
        BaseType<String> type = new NoOpType("encoded\0String");
        TypedValue value = new TypedValue(type);
        String actual = serialize(value);
        assertEquals(expected64("xs:string", new String(Base64.encodeBase64("encoded\0String".getBytes()))), actual);
        assertTrue(value.isBase64Encoded());
        assertEquals("xs:string", value.getType());
    }
    
    @Test
    public void testEncodedTypeString() throws Exception {
        BaseType<String> type = new LcNoDiacriticsType("encoded\0String");
        TypedValue value = new TypedValue(type);
        String actual = serialize(value);
        assertEquals(expected64("xs:string", new String(Base64.encodeBase64("encoded\0String".getBytes()))), actual);
        assertTrue(value.isBase64Encoded());
        assertEquals("xs:string", value.getType());
    }
    
    @Test
    public void testBoolean() throws Exception {
        TypedValue value = new TypedValue(Boolean.TRUE);
        String actual = serialize(value);
        assertEquals(expected("xs:boolean", "true"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:boolean", value.getType());
    }
    
    @Test
    public void testShort() throws Exception {
        TypedValue value = new TypedValue((short) 42);
        String actual = serialize(value);
        assertEquals(expected("xs:short", "42"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:short", value.getType());
    }
    
    @Test
    public void testInteger() throws Exception {
        TypedValue value = new TypedValue((int) 42);
        String actual = serialize(value);
        assertEquals(expected("xs:int", "42"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:int", value.getType());
    }
    
    @Test
    public void testLong() throws Exception {
        TypedValue value = new TypedValue(42L);
        String actual = serialize(value);
        assertEquals(expected("xs:long", "42"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:long", value.getType());
    }
    
    @Test
    public void testFloat() throws Exception {
        TypedValue value = new TypedValue(42.42f);
        String actual = serialize(value);
        assertEquals(expected("xs:float", "42.42"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:float", value.getType());
    }
    
    @Test
    public void testDouble() throws Exception {
        TypedValue value = new TypedValue(42.42);
        String actual = serialize(value);
        assertEquals(expected("xs:double", "42.42"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:double", value.getType());
    }
    
    @Test
    public void testBigDecimal() throws Exception {
        TypedValue value = new TypedValue(new BigDecimal("123456789012345678901234567890.123"));
        String actual = serialize(value);
        assertEquals(expected("xs:decimal", "123456789012345678901234567890.123"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:decimal", value.getType());
    }
    
    @Test
    public void testBigInteger() throws Exception {
        TypedValue value = new TypedValue(new BigInteger("123456789012345678901234567890"));
        String actual = serialize(value);
        assertEquals(expected("xs:integer", "123456789012345678901234567890"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:integer", value.getType());
    }
    
    @Test
    public void testByte() throws Exception {
        TypedValue value = new TypedValue((byte) 42);
        String actual = serialize(value);
        assertEquals(expected("xs:byte", "42"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:byte", value.getType());
    }
    
    @Test
    public void testByteArray() throws Exception {
        TypedValue value = new TypedValue(new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 42});
        String actual = serialize(value);
        assertEquals(expected("xs:base64Binary", new String(Base64.encodeBase64((byte[]) value.getValue()))), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:base64Binary", value.getType());
    }
    
    @Test
    public void testDateTime() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("EST"));
        cal.set(2012, 0, 25, 14, 34, 35);
        cal.set(Calendar.MILLISECOND, 525);
        TypedValue value = new TypedValue(cal);
        String actual = serialize(value);
        assertEquals(expected("xs:dateTime", "2012-01-25T14:34:35.525-05:00"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:dateTime", value.getType());
    }
    
    @Test
    public void testDate() throws Exception {
        Date d = new Date(1408552225400L);
        TypedValue value = new TypedValue(d);
        String actual = serialize(value);
        assertEquals(expected("xs:dateTime", "2014-08-20T16:30:25.400Z"), actual);
        assertFalse(value.isBase64Encoded());
        assertEquals("xs:dateTime", value.getType());
    }
    
    private String expected(String xsdType, String value) {
        return MessageFormat.format(EXPECTED_FORMAT, xsdType, value);
    }
    
    private String expected64(String xsdType, String value) {
        return MessageFormat.format(EXPECTED_BASE64_FORMAT, xsdType, value);
    }
    
    private String serialize(TypedValue value) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ctx.createMarshaller().marshal(value, bos);
        return bos.toString();
    }
    
}
