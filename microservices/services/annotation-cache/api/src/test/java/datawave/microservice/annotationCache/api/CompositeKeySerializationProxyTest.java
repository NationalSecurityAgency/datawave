package datawave.microservice.annotationCache.api;

import static java.io.ObjectStreamConstants.SC_SERIALIZABLE;
import static java.io.ObjectStreamConstants.STREAM_MAGIC;
import static java.io.ObjectStreamConstants.STREAM_VERSION;
import static java.io.ObjectStreamConstants.TC_CLASSDESC;
import static java.io.ObjectStreamConstants.TC_ENDBLOCKDATA;
import static java.io.ObjectStreamConstants.TC_NULL;
import static java.io.ObjectStreamConstants.TC_OBJECT;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

/** Tests canonical Java serialization and validation of composite-key proxies. */
class CompositeKeySerializationProxyTest {
    /** Checks that both keys serialize through private proxies and deserialize as validated keys. */
    @Test
    void javaSerializationAutomaticallyReplacesAndResolvesBothKeys() throws Exception {
        for (Object key : keys("TYPE", "document", "identity")) {
            Object proxy = proxyFor(key);
            assertTrue(Modifier.isPrivate(proxy.getClass().getModifiers()));
            assertTrue(Modifier.isStatic(proxy.getClass().getModifiers()), "proxy must not capture the original key");
            var serializedFields = ObjectStreamClass.lookup(proxy.getClass()).getFields();
            assertEquals(1, serializedFields.length, "only the canonical byte array may enter the serialized graph");
            assertEquals("payload", serializedFields[0].getName());
            assertEquals(byte[].class, serializedFields[0].getType());
            assertEquals(1L, ObjectStreamClass.lookup(proxy.getClass()).getSerialVersionUID());
            assertEquals(1L, ObjectStreamClass.lookup(key.getClass()).getSerialVersionUID());

            Object copy = roundTrip(key);
            assertEquals(key.getClass(), copy.getClass(), "readResolve must return the real key, not its proxy");
            assertEquals(key, copy);
            assertEquals(key.hashCode(), copy.hashCode());
            assertNotSame(key, copy);
            assertEquals(key, roundTrip(proxy));
            for (Field field : key.getClass().getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    assertTrue(Modifier.isPrivate(field.getModifiers()));
                    assertTrue(Modifier.isFinal(field.getModifiers()));
                }
            }
        }
    }

    /** Checks that equal tuples have identical serialized bytes regardless of string sharing. */
    @Test
    void equalTuplesHaveCanonicalJavaBytesRegardlessOfStringSharing() throws Exception {
        String shared = new String("same-value");
        String[][] tuples = {{"UUID", shared, shared}, {shared, "document", shared}, {shared, shared, "identity"}, {shared, shared, shared},
                {"interned", "interned", "interned"}, {"left|:right", "document\u0000雪\uD83D\uDE80", "unpaired-\uD800-\uDC00"},
                {"long-" + "x".repeat(70_000), "雪".repeat(70_000), "identity"}};
        for (String[] tuple : tuples) {
            Object[] originals = keys(tuple[0], tuple[1], tuple[2]);
            Object[] equivalents = keys(new String(tuple[0]), new String(tuple[1]), new String(tuple[2]));
            for (int index = 0; index < originals.length; index++) {
                assertEquals(originals[index], equivalents[index]);
                assertArrayEquals(bytes(originals[index]), bytes(equivalents[index]));
                assertEquals(originals[index], roundTrip(originals[index]));
                assertEquals(equivalents[index], roundTrip(equivalents[index]));
            }
            assertFalse(Arrays.equals(bytes(originals[0]), bytes(originals[1])), "different key types must retain different binary identities");
        }
    }

    /** Checks the tuple format preserves field order, version, and exact UTF-16 characters. */
    @Test
    void tupleEncodingHasExplicitVersionLengthsOrderAndExactUtf16CodeUnits() throws Exception {
        byte[] expected = {1, 0, 0, 0, 1, 0, 65, 0, 0, 0, 1, (byte) 0x96, (byte) 0xea, 0, 0, 0, 2, (byte) 0xd8, 0x3d, (byte) 0xde, (byte) 0x80};
        assertArrayEquals(expected, KeyTupleEncoding.encode("A", "雪", "\uD83D\uDE80"));
        assertArrayEquals(new String[] {"A", "雪", "\uD83D\uDE80"}, KeyTupleEncoding.decode(expected));
        String unusual = "\u0000\uD800x\uDC00";
        assertArrayEquals(new String[] {unusual, "second", "third"}, KeyTupleEncoding.decode(KeyTupleEncoding.encode(unusual, "second", "third")));
    }

    /** Checks that truncated, malformed, and extra payload data is rejected. */
    @Test
    void malformedPayloadsAreRejectedBeforeAllocatingDeclaredLengths() throws Exception {
        byte[] valid = KeyTupleEncoding.encode("TYPE", "document", "identity");
        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
        byte[] negativeLength = ByteBuffer.allocate(5).put((byte) 1).putInt(-1).array();
        byte[] oversizedLength = ByteBuffer.allocate(5).put((byte) 1).putInt(Integer.MAX_VALUE).array();
        byte[][] invalidPayloads = {null, new byte[0], {2}, {1}, negativeLength, oversizedLength, Arrays.copyOf(valid, valid.length - 1), trailing};
        for (byte[] payload : invalidPayloads) {
            assertThrows(InvalidObjectException.class, () -> KeyTupleEncoding.decode(payload));
            for (Object key : keys("TYPE", "document", "identity")) {
                Object proxy = proxyWithPayload(key, payload);
                assertThrows(InvalidObjectException.class, () -> roundTrip(proxy));
            }
        }
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            assertThrows(InvalidObjectException.class, () -> KeyTupleEncoding.decode(truncated));
        }
    }

    /** Checks that deserialized key fields still pass the public constructor's validation. */
    @Test
    void proxyResolutionReusesConstructorValidationForEveryComponent() throws Exception {
        String[][] invalidTuples = {{"", "document", "identity"}, {"TYPE", " \t", "identity"}, {"TYPE", "document", "\n"}};
        for (String[] tuple : invalidTuples) {
            for (Object key : keys("TYPE", "document", "identity")) {
                Object proxy = proxyWithPayload(key, KeyTupleEncoding.encode(tuple[0], tuple[1], tuple[2]));
                InvalidObjectException failure = assertThrows(InvalidObjectException.class, () -> roundTrip(proxy));
                assertInstanceOf(IllegalArgumentException.class, failure.getCause());
            }
        }
    }

    /** Checks that Java serialization rejects keys that do not use their proxy. */
    @Test
    void rejectsDirectKeyDeserializationWithoutAProxy() throws Exception {
        for (Class<?> keyType : new Class<?>[] {AnnotationKey.class, FetchKey.class}) {
            // A minimal valid Java stream for a direct key, deliberately bypassing writeReplace.
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeShort(STREAM_MAGIC);
                out.writeShort(STREAM_VERSION);
                out.writeByte(TC_OBJECT);
                out.writeByte(TC_CLASSDESC);
                out.writeUTF(keyType.getName());
                out.writeLong(1L);
                out.writeByte(SC_SERIALIZABLE);
                out.writeShort(0);
                out.writeByte(TC_ENDBLOCKDATA);
                out.writeByte(TC_NULL);
            }
            try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                InvalidObjectException failure = assertThrows(InvalidObjectException.class, input::readObject);
                assertEquals("Serialization proxy required", failure.getMessage());
            }
        }
    }

    private static Object[] keys(String first, String second, String third) {
        return new Object[] {new AnnotationKey(first, second, third), new FetchKey(first, second, third)};
    }

    private static Object proxyFor(Object key) throws Exception {
        Method replacement = key.getClass().getDeclaredMethod("writeReplace");
        replacement.setAccessible(true);
        return replacement.invoke(key);
    }

    private static Object proxyWithPayload(Object key, byte[] payload) throws Exception {
        Object proxy = proxyFor(key);
        Field field = proxy.getClass().getDeclaredField("payload");
        field.setAccessible(true);
        field.set(proxy, payload);
        return proxy;
    }

    private static Object roundTrip(Object value) throws Exception {
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes(value)))) {
            return input.readObject();
        }
    }

    private static byte[] bytes(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        return bytes.toByteArray();
    }
}
