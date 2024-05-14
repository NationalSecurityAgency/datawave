package datawave.query.util.count;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Utility class for serializing and deserializing a {@link CountMap}
 */
public class CountMapSerDe {

    private final Kryo kryo;
    private final ByteArrayOutputStream baos;

    public CountMapSerDe() {
        kryo = new Kryo();
        baos = new ByteArrayOutputStream(4096);
    }

    public String serializeToString(CountMap map) {
        return new String(serialize(map), StandardCharsets.ISO_8859_1);
    }

    public byte[] serialize(CountMap map) {
        baos.reset();
        Output output = new Output(baos);
        kryo.writeObject(output, map);
        output.close();
        return baos.toByteArray();
    }

    public CountMap deserializeFromString(String data) {
        return deserialize(data.getBytes(StandardCharsets.ISO_8859_1));
    }

    public CountMap deserialize(byte[] data) {
        Input input = new Input(data);
        CountMap map = kryo.readObject(input, CountMap.class);
        input.close();

        if (map == null) {
            throw new RuntimeException("Deserialized null CountMap");
        }

        return map;
    }
}
