package datawave.query.index.day;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Utility class for serialized and deserializing {@link BitSetIndexEntry}
 */
public class BitSetIndexEntrySerializer {

    private final Kryo kryo;
    private final ByteArrayOutputStream baos;

    public BitSetIndexEntrySerializer() {
        kryo = new Kryo();
        baos = new ByteArrayOutputStream(4096);
    }

    public String serializeToString(BitSetIndexEntry entry) {
        return new String(serialize(entry), StandardCharsets.ISO_8859_1);
    }

    public byte[] serialize(BitSetIndexEntry entry) {
        baos.reset();
        Output output = new Output(baos);
        kryo.writeObject(output, entry);
        output.close();
        return baos.toByteArray();
    }

    public BitSetIndexEntry deserializeFromString(String data) {
        return deserialize(data.getBytes(StandardCharsets.ISO_8859_1));
    }

    public BitSetIndexEntry deserialize(byte[] data) {
        Input input = new Input(data);
        BitSetIndexEntry entry = kryo.readObject(input, BitSetIndexEntry.class);
        input.close();

        if (entry == null) {
            throw new RuntimeException("Deserialized null BitSetIndexEntry");
        }
        return entry;
    }
}
