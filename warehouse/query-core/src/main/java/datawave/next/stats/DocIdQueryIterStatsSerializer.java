package datawave.next.stats;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Serializer for {@link DocIdQueryIterStats} that uses {@link Kryo}
 */
public class DocIdQueryIterStatsSerializer {

    private final Kryo kryo = new Kryo();
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public byte[] serialize(DocIdQueryIterStats stats) {
        baos.reset();
        try (var output = new Output(baos)) {
            stats.write(kryo, output);
        }
        return baos.toByteArray();
    }

    public DocIdQueryIterStats deserialize(byte[] bytes) {
        DocIdQueryIterStats stats = new DocIdQueryIterStats();
        try (var input = new Input(bytes)) {
            stats.read(kryo, input);
        }
        return stats;
    }
}
