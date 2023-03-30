package datawave.query.attributes.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.ingest.data.Type;

public class DelegateStringTypeSerializer extends Serializer<Type> {
    @Override
    public void write(Kryo kryo, Output output, Type object) {
        
    }
    
    @Override
    public Type read(Kryo kryo, Input input, Class<Type> type) {
        return null;
    }
}
