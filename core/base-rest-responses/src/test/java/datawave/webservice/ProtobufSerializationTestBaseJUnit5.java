package datawave.webservice;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;

import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtobufIOUtil;

public class ProtobufSerializationTestBaseJUnit5 {
    protected LinkedBuffer buffer;

    @BeforeEach
    public void setUp() {
        buffer = LinkedBuffer.allocate(4096);
    }

    protected <T extends Message<T>> void testFieldNames(String[] fieldNames, Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        assertEquals(fieldNames.length, fields.length,
                        "The number of fields in " + clazz.getName() + " has changed. Please update " + getClass().getName() + ".");

        String[] actualFieldNames = new String[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            actualFieldNames[i] = fields[i].getName();
        }

        Arrays.sort(fieldNames);
        Arrays.sort(actualFieldNames);
        assertArrayEquals(fieldNames, actualFieldNames, "Serialization/deserialization of " + clazz.getName() + " failed.");
    }

    protected <T extends Message<T>> void testRoundTrip(Class<T> clazz, String[] fieldNames, Object[] fieldValues) throws Exception {
        assertNotNull(fieldNames);
        assertNotNull(fieldValues);
        assertEquals(fieldNames.length, fieldValues.length);

        T original = clazz.getDeclaredConstructor().newInstance();
        for (int i = 0; i < fieldNames.length; ++i) {
            Field field = clazz.getDeclaredField(fieldNames[i]);
            field.setAccessible(true);
            field.set(original, fieldValues[i]);
        }

        T reconstructed = roundTrip(original);
        for (int i = 0; i < fieldNames.length; ++i) {
            Field field = clazz.getDeclaredField(fieldNames[i]);
            field.setAccessible(true);
            assertEquals(fieldValues[i], field.get(reconstructed));
        }
    }

    protected <T extends Message<T>> T roundTrip(T message) {
        byte[] bytes = toProtobufBytes(message);
        T response = message.cachedSchema().newMessage();
        ProtobufIOUtil.mergeFrom(bytes, response, message.cachedSchema());
        return response;
    }

    protected <T extends Message<T>> byte[] toProtobufBytes(T message) {
        return ProtobufIOUtil.toByteArray(message, message.cachedSchema(), buffer);
    }
}
