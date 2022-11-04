package datawave.user;

import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtobufIOUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * UserAuthorizationsTest is the only test that extends this one, so co-located it and removed dependency on microservice-reset-api test jar
 */
public class ProtobufSerializationTestBase {
    protected LinkedBuffer buffer;
    
    public ProtobufSerializationTestBase() {}
    
    @BeforeEach
    public void setUp() {
        this.buffer = LinkedBuffer.allocate(4096);
    }
    
    protected <T extends Message<T>> void testFieldNames(String[] fieldNames, Class<T> clazz) throws InstantiationException, IllegalAccessException {
        T original = clazz.newInstance();
        Field[] fields = original.getClass().getDeclaredFields();
        Assertions.assertEquals(fieldNames.length, fields.length, "The number of fields in " + clazz.getName() + " has changed.  Please update "
                        + this.getClass().getName() + ".");
        String[] actualFieldNames = new String[fields.length];
        
        for (int i = 0; i < fields.length; ++i) {
            actualFieldNames[i] = fields[i].getName();
        }
        
        Arrays.sort(fieldNames);
        Arrays.sort(actualFieldNames);
        Assertions.assertArrayEquals(fieldNames, actualFieldNames, "Serialization/deserialization of " + clazz.getName() + " failed.");
    }
    
    protected <T extends Message<T>> void testRoundTrip(Class<T> clazz, String[] fieldNames, Object[] fieldValues) throws Exception {
        Assertions.assertNotNull(fieldNames);
        Assertions.assertNotNull(fieldValues);
        Assertions.assertEquals((long) fieldNames.length, (long) fieldValues.length);
        T original = clazz.newInstance();
        
        for (int i = 0; i < fieldNames.length; ++i) {
            Field f = original.getClass().getDeclaredField(fieldNames[i]);
            f.setAccessible(true);
            f.set(original, fieldValues[i]);
        }
        
        T reconstructed = this.roundTrip(original);
        
        for (int i = 0; i < fieldNames.length; ++i) {
            Field f = reconstructed.getClass().getDeclaredField(fieldNames[i]);
            f.setAccessible(true);
            Assertions.assertEquals(fieldValues[i], f.get(reconstructed));
        }
        
    }
    
    protected <T extends Message<T>> T roundTrip(T message) throws Exception {
        byte[] bytes = this.toProtobufBytes(message);
        T response = message.cachedSchema().newMessage();
        ProtobufIOUtil.mergeFrom(bytes, response, message.cachedSchema());
        return response;
    }
    
    protected <T extends Message<T>> byte[] toProtobufBytes(T message) {
        return ProtobufIOUtil.toByteArray(message, message.cachedSchema(), this.buffer);
    }
}
