package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import datawave.data.type.DelegateTypeSerializer;
import datawave.data.type.StringType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class DelegateTypeSerializerTest {
    private final static Serializer<String> STRING_DELEGATE_SERIALIZER = new DefaultSerializers.StringSerializer();

    @Test
    public void testStringTypeSerializeAndDeserialize() {
        DelegateTypeSerializer<String, StringType> serializer = new DelegateTypeSerializer<>(STRING_DELEGATE_SERIALIZER, String.class);
        Kryo kryoOutput = new Kryo();
        ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
        Output output = new Output(outputBytes);
        StringType expectedType = new StringType();
        expectedType.setDelegateFromString("TEST_Value");

        serializer.write(kryoOutput, output, expectedType);
        output.flush();

        Kryo kryoInput = new Kryo();
        Input input = new Input(new ByteArrayInputStream(outputBytes.toByteArray()));
        StringType actualType = serializer.read(kryoInput, input, StringType.class);

        Assert.assertEquals(expectedType.getDelegate(), actualType.getDelegate());
        Assert.assertEquals(expectedType.getNormalizedValue(), actualType.getNormalizedValue());
    }
}
