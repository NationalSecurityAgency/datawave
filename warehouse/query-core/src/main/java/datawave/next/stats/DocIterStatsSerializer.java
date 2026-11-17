package datawave.next.stats;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Serializer for {@link DocIterStats} that uses {@link Kryo}
 */
public class DocIterStatsSerializer {

    private final Kryo kryo = new Kryo();
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public byte[] serialize(DocIterStats stats) {
        baos.reset();
        try (var output = new Output(baos)) {
            stats.write(kryo, output);
        }
        return baos.toByteArray();
    }

    public DocIterStats deserialize(byte[] bytes) {
        DocIterStats stats = new DocIterStats();
        try (var input = new Input(bytes)) {
            stats.read(kryo, input);
        }
        return stats;
    }
}
