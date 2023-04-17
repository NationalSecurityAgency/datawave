package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.data.type.IpAddressType;
import datawave.data.type.util.IpAddress;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class IpAddressDynamicSerializerTest {

    @Test
    public void testIpAddress() {
        IpAddressDynamicSerializer serializer = new IpAddressDynamicSerializer();
        Kryo kryoOutput = new Kryo();
        ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
        Output output = new Output(outputBytes);
        IpAddressType expectedType = new IpAddressType();
        expectedType.setDelegateFromString(expectedType.normalize("10.0.0.*"));

        serializer.write(kryoOutput, output, expectedType.getDelegate());
        output.flush();

        Kryo kryoInput = new Kryo();
        Input input = new Input(new ByteArrayInputStream(outputBytes.toByteArray()));
        IpAddress actualType = serializer.read(kryoInput, input, IpAddress.class);

        Assert.assertEquals(expectedType.getDelegate(), actualType);
    }
}
