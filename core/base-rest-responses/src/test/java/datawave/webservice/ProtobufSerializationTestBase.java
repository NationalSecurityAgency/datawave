package datawave.webservice;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.support.membermodification.MemberMatcher.field;
import static org.powermock.api.support.membermodification.MemberMatcher.fields;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.junit.Before;

import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtobufIOUtil;

public class ProtobufSerializationTestBase {
    protected LinkedBuffer buffer;
    
    @Before
    public void setUp() {
        buffer = LinkedBuffer.allocate(4096);
    }
    
    protected <T extends Message<T>> void testFieldNames(String[] fieldNames, Class<T> clazz) {
        Field[] fields = fields(clazz);
        assertEquals("The number of fields in " + clazz.getName() + " has changed.  Please update " + getClass().getName() + ".", fieldNames.length,
                        fields.length);
        
        String[] actualFieldNames = new String[fields.length];
        for (int i = 0; i < fields.length; ++i)
            actualFieldNames[i] = fields[i].getName();
        
        Arrays.sort(fieldNames);
        Arrays.sort(actualFieldNames);
        assertArrayEquals("Serialization/deserialization of " + clazz.getName() + " failed.", fieldNames, actualFieldNames);
    }
    
    protected <T extends Message<T>> void testRoundTrip(Class<T> clazz, String[] fieldNames, Object[] fieldValues) throws Exception {
        assertNotNull(fieldNames);
        assertNotNull(fieldValues);
        assertEquals(fieldNames.length, fieldValues.length);
        
        T original = clazz.getDeclaredConstructor().newInstance();
        for (int i = 0; i < fieldNames.length; ++i)
            field(clazz, fieldNames[i]).set(original, fieldValues[i]);
        
        T reconstructed = roundTrip(original);
        for (int i = 0; i < fieldNames.length; ++i)
            assertEquals(fieldValues[i], field(clazz, fieldNames[i]).get(reconstructed));
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
