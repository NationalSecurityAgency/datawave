package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.data.type.util.IpV4Address;

public class IPV4AddressSerializer extends Serializer<IpV4Address> {
    @Override
    public void write(Kryo kryo, Output output, IpV4Address object) {
        output.writeInt(object.getAddress().length, true);
        output.writeBytes(object.getAddress());
        output.writeInt(object.getNumOctets(), true);
        output.writeInt(object.getWildcardLoc(), false);
    }

    @Override
    public IpV4Address read(Kryo kryo, Input input, Class<IpV4Address> type) {
        int addressSize = input.readInt(true);
        byte[] address = input.readBytes(addressSize);
        int octets = input.readInt(true);
        int wildcardLoc = input.readInt(false);
        return new IpV4Address(address, octets, wildcardLoc, false);
    }
}
