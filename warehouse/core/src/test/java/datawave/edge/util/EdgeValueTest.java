package datawave.edge.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;

import datawave.edge.util.EdgeValue.EdgeValueBuilder;

import org.apache.accumulo.core.data.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class EdgeValueTest {
    
    @Before
    public void setUp() throws Exception {}
    
    @After
    public void tearDown() throws Exception {}
    
    @Test
    public void testNewBuilder() throws InvalidProtocolBufferException {
        EdgeValueBuilder builder = EdgeValue.newBuilder();
        builder.setBitmask(1);
        builder.setCount(42l);
        
        EdgeValue eValue = builder.build();
        Value value = eValue.encode();
        
        EdgeValue outValue = EdgeValue.decode(value);
        
        assertTrue(outValue.hasCount());
        assertTrue(outValue.hasBitmask());
        assertEquals(42, (long) outValue.getCount());
        assertEquals(1, (int) outValue.getBitmask());
        
        // new builder from EdgeValue.
        builder = EdgeValue.newBuilder(eValue);
        EdgeValue outValue2 = EdgeValue.decode(builder.build().encode());
        
        assertTrue(outValue2.hasCount());
        assertTrue(outValue2.hasBitmask());
        assertEquals(42, (long) outValue2.getCount());
        assertEquals(1, (int) outValue2.getBitmask());
    }
    
    @Test
    public void testInvalidUuidSavedAsStringOnly() throws InvalidProtocolBufferException {
        String invalidUuid = "23423";
        EdgeValueBuilder builder = EdgeValue.newBuilder();
        builder.setUuid(invalidUuid);
        EdgeValue eValue = builder.build();
        assertNull("Builder should return null UUID object if uuid string wasn't convertable", builder.getUuidObj());
        
        Value value = eValue.encode();
        assertNull("EdgeValue should return null UUID object if uuid string wasn't convertable", eValue.getUuidObject());
        
        EdgeValue outValue = EdgeValue.decode(value);
        assertEquals(invalidUuid, outValue.getUuid());
        assertNull("Decoded protobuf EdgeValue should have null uuid Object", outValue.getUuidObject());
    }
    
    @Test
    public void testBuilderCreatesUuidObjUponRequest() throws InvalidProtocolBufferException {
        String validUuidString = "11111111-1111-1111-1111-111111111111";
        EdgeValueBuilder builder = EdgeValue.newBuilder();
        builder.setUuid(validUuidString);
        EdgeValue eValue = builder.build();
        assertEquals("Builder should convert uuid string to UUID object if UUID object was requested",
                        EdgeValue.convertUuidStringToUuidObj("11111111-1111-1111-1111-111111111111"), builder.getUuidObj());
    }
    
    @Test
    public void testEdgeValueCreatesUuidObjUponRequest() throws InvalidProtocolBufferException {
        String validUuidString = "11111111-1111-1111-1111-111111111111";
        EdgeValue eValue = createEdgeValueWithUuid(validUuidString);
        
        Value value = eValue.encode();
        assertNotNull("EdgeValue should convert uuid string to UUID object if UUID object was requested", eValue.getUuidObject());
        
        EdgeValue outValue = EdgeValue.decode(value);
        assertEquals(validUuidString, outValue.getUuid());
        assertNotNull(outValue.getUuidObject());
    }
    
    @Test
    public void testEncodeUuidStringDecodeCreatesUuidObj() throws InvalidProtocolBufferException {
        String validUuidString = "11111111-1111-1111-1111-111111111111";
        EdgeValue eValue = createEdgeValueWithUuid(validUuidString);
        
        Value value = eValue.encode();
        
        EdgeValue outValue = EdgeValue.decode(value);
        assertEquals(validUuidString, outValue.getUuid());
        assertNotNull(outValue.getUuidObject());
    }
    
    @Test
    public void testEqualAfterReencodingWithValidUuid() throws InvalidProtocolBufferException {
        EdgeValue originalEdgeValue = createEdgeValueWithUuid("11111111-1111-1111-1111-111111111111");
        EdgeValue decodedEdgeValue = EdgeValue.decode(createEdgeValueWithUuid("11111111-1111-1111-1111-111111111111").encode());
        assertEquals(originalEdgeValue + "\n" + decodedEdgeValue, originalEdgeValue, decodedEdgeValue);
    }
    
    @Test
    public void testEqualAfterReencodingWithInvalidUuid() throws InvalidProtocolBufferException {
        EdgeValue originalEdgeValue = createEdgeValueWithUuid("1234");
        EdgeValue decodedEdgeValue = EdgeValue.decode(createEdgeValueWithUuid("1234").encode());
        assertEquals(originalEdgeValue + "\n" + decodedEdgeValue, originalEdgeValue, decodedEdgeValue);
    }
    
    @Test
    public void testNotEqualUuid() throws InvalidProtocolBufferException {
        EdgeValue originalEdgeValue = createEdgeValueWithUuid("1234");
        EdgeValue decodedEdgeValue = EdgeValue.decode(createEdgeValueWithUuid("11111111-1111-1111-1111-111111111111").encode());
        assertNotEquals(originalEdgeValue, decodedEdgeValue);
    }
    
    @Test
    public void testHashcodeAfterReencodingWithValidUuid() throws InvalidProtocolBufferException {
        EdgeValue originalEdgeValue = createEdgeValueWithUuid("11111111-1111-1111-1111-111111111111");
        EdgeValue decodedEdgeValue = EdgeValue.decode(createEdgeValueWithUuid("11111111-1111-1111-1111-111111111111").encode());
        assertEquals(originalEdgeValue + "\n" + decodedEdgeValue, originalEdgeValue.hashCode(), decodedEdgeValue.hashCode());
    }
    
    @Test
    public void testHashcodeAfterReencodingWithInvalidUuid() throws InvalidProtocolBufferException {
        EdgeValue originalEdgeValue = createEdgeValueWithUuid("1234");
        EdgeValue decodedEdgeValue = EdgeValue.decode(createEdgeValueWithUuid("1234").encode());
        assertEquals(originalEdgeValue + "\n" + decodedEdgeValue, originalEdgeValue.hashCode(), decodedEdgeValue.hashCode());
    }
    
    @Test
    public void testHashcodeDiffer() throws InvalidProtocolBufferException {
        EdgeValue originalEdgeValue = createEdgeValueWithUuid("1234");
        EdgeValue decodedEdgeValue = EdgeValue.decode(createEdgeValueWithUuid("11111111-1111-1111-1111-111111111111").encode());
        assertNotEquals(originalEdgeValue.hashCode(), decodedEdgeValue.hashCode());
    }
    
    private EdgeValue createEdgeValueWithUuid(String validUuidString) {
        EdgeValueBuilder builder = EdgeValue.newBuilder();
        builder.setUuid(validUuidString);
        return builder.build();
    }
    
    @Test
    public void testBitmask() {
        EdgeValueBuilder builder = EdgeValue.newBuilder();
        builder.setBitmask(1);
        builder.setHour(4);
        builder.setHour(23);
        builder.combineBitmask(0x000400);
        
        EdgeValue eValue = builder.build();
        
        assertTrue(eValue.hasBitmask());
        assertFalse(eValue.hasCount());
        
        int outMask = eValue.getBitmask();
        
        assertTrue((outMask == 8389649));
    }
    
    @Test
    public void testFileDecodeEncode() throws Exception {
        EdgeValueBuilder builder = EdgeValue.newBuilder();
        builder.setSourceValue("source<ds1>");
        builder.setSinkValue("sink<ds2>");
        builder.setBitmask(0x801);
        builder.setCount((long) (0x8010));
        builder.setDuration(new ArrayList<>(Arrays.asList(5L, 4L)));
        builder.setHours(new ArrayList<>(Arrays.asList(12L, 13L)));
        
        Value value = builder.build().encode();
        
        File file = File.createTempFile("EdgeValueTest", "bytes");
        OutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
        stream.write(value.get(), 0, value.getSize());
        stream.close();
        
        InputStream istream = new BufferedInputStream(new FileInputStream(file));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int size = istream.read(buffer, 0, 1024);
        while (size >= 0) {
            bytes.write(buffer, 0, size);
            size = istream.read(buffer, 0, 1024);
        }
        istream.close();
        value = new Value(bytes.toByteArray(), 0, bytes.size());
        
        EdgeValue eValue = EdgeValue.decode(value);
        
        assertEquals("source<ds1>", eValue.getSourceValue());
        assertEquals("sink<ds2>", eValue.getSinkValue());
        assertEquals(0x801, eValue.getBitmask().intValue());
        assertEquals(0x8010, eValue.getCount().longValue());
        assertEquals(new ArrayList<>(Arrays.asList(5L, 4L)), eValue.getDuration());
        assertEquals(new ArrayList<>(Arrays.asList(12L, 13L)), eValue.getHours());
    }
    
    @Test
    public void testFileCompatibility() throws Exception {
        InputStream stream = new BufferedInputStream(ClassLoader.getSystemResourceAsStream("EdgeValue.2.x.bytes"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int size = stream.read(buffer, 0, 1024);
        while (size >= 0) {
            bytes.write(buffer, 0, size);
            size = stream.read(buffer, 0, 1024);
        }
        stream.close();
        Value value = new Value(bytes.toByteArray(), 0, bytes.size());
        
        EdgeValue eValue = EdgeValue.decode(value);
        
        assertEquals("source<ds1>", eValue.getSourceValue());
        assertEquals("sink<ds2>", eValue.getSinkValue());
        assertEquals(0x801, eValue.getBitmask().intValue());
        assertEquals(0x8010, eValue.getCount().longValue());
        assertEquals(new ArrayList<>(Arrays.asList(5L, 4L)), eValue.getDuration());
        assertEquals(new ArrayList<>(Arrays.asList(12L, 13L)), eValue.getHours());
        
        stream = new BufferedInputStream(ClassLoader.getSystemResourceAsStream("EdgeValue.3.x.bytes"));
        bytes = new ByteArrayOutputStream();
        size = stream.read(buffer, 0, 1024);
        while (size >= 0) {
            bytes.write(buffer, 0, size);
            size = stream.read(buffer, 0, 1024);
        }
        stream.close();
        value = new Value(bytes.toByteArray(), 0, bytes.size());
        
        eValue = EdgeValue.decode(value);
        
        assertEquals("source<ds1>", eValue.getSourceValue());
        assertEquals("sink<ds2>", eValue.getSinkValue());
        assertEquals(0x801, eValue.getBitmask().intValue());
        assertEquals(0x8010, eValue.getCount().longValue());
        assertEquals(new ArrayList<>(Arrays.asList(5L, 4L)), eValue.getDuration());
        assertEquals(new ArrayList<>(Arrays.asList(12L, 13L)), eValue.getHours());
    }
    
}
