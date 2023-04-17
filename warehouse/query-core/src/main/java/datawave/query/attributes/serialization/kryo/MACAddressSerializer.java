package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.data.type.util.MACAddress;

public class MACAddressSerializer extends Serializer<MACAddress> {
    @Override
    public void write(Kryo kryo, Output output, MACAddress object) {
        output.writeString(object.getMacAddress());
        output.writeString(object.getSeparator());
        output.writeInt(object.getGroupingSize(), true);
    }

    @Override
    public MACAddress read(Kryo kryo, Input input, Class<MACAddress> type) {
        String address = input.readString();
        String separator = input.readString();
        int groupingSize = input.readInt(true);
        return new MACAddress(address, separator, groupingSize);
    }
}
