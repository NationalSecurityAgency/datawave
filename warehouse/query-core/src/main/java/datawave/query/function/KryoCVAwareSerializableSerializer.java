package datawave.query.function;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer;

import datawave.query.attributes.Document;

public class KryoCVAwareSerializableSerializer extends KryoSerializableSerializer {
    private Boolean reducedResponse = true;

    public KryoCVAwareSerializableSerializer(Boolean reducedResponse) {
        setReducedResponse(reducedResponse);
    }

    public Boolean getReducedResponse() {
        return reducedResponse;
    }

    public void setReducedResponse(Boolean reducedResponse) {
        this.reducedResponse = reducedResponse;
    }

    @Override
    public void write(Kryo kryo, Output output, KryoSerializable object) {
        if (object instanceof Document) {
            ((Document) object).write(kryo, output);
        } else {
            object.write(kryo, output);
        }
    }

}
