package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.data.type.util.IpV6Address;

public class IPV6AddressSerializer extends Serializer<IpV6Address> {
    @Override
    public void write(Kryo kryo, Output output, IpV6Address object) {
        short[] address = object.getAddress();
        output.writeShort(address[0]);
        output.writeShort(address[1]);
        output.writeShort(address[2]);
        output.writeShort(address[3]);
        output.writeShort(address[4]);
        output.writeShort(address[5]);
        output.writeShort(address[6]);
        output.writeShort(address[7]);
    }

    @Override
    public IpV6Address read(Kryo kryo, Input input, Class<IpV6Address> type) {
        short[] address = new short[8];
        address[0] = input.readShort();
        address[1] = input.readShort();
        address[2] = input.readShort();
        address[3] = input.readShort();
        address[4] = input.readShort();
        address[5] = input.readShort();
        address[6] = input.readShort();
        address[7] = input.readShort();
        return new IpV6Address(address, false);
    }
}
