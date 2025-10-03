package datawave.query.util.count;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

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
        return new String(Base64.getEncoder().encode(serialize(map)), StandardCharsets.ISO_8859_1);
    }

    public byte[] serialize(CountMap map) {
        baos.reset();
        Output output = new Output(baos);
        map.write(kryo, output);
        output.close();
        return baos.toByteArray();
    }

    public CountMap deserializeFromString(String data) {
        return deserialize(Base64.getDecoder().decode(data));
    }

    public CountMap deserialize(byte[] data) {
        CountMap map;
        try (Input input = new Input(data)) {
            map = new CountMap();
            map.read(kryo, input);
        }
        return map;
    }
}
