package datawave.query.util;

import java.io.ByteArrayOutputStream;
import java.util.Base64;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Serializer for {@link TypeMetadata} that uses its native {@link KryoSerializable} methods, encoded to and from a Base64 string so the result can be carried
 * anywhere a {@link String} is expected, such as an Accumulo {@code IteratorSetting} option value. Not thread-safe; each thread that serializes or deserializes
 * {@link TypeMetadata} should hold its own instance.
 */
public class TypeMetadataSerializer {

    private final Kryo kryo = new Kryo();
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    /**
     * Serializes the given {@link TypeMetadata} to a Base64-encoded Kryo byte stream.
     *
     * @param typeMetadata
     *            the instance to serialize
     * @return a Base64-encoded string suitable for use as an iterator option value
     */
    public String serialize(TypeMetadata typeMetadata) {
        baos.reset();
        try (Output output = new Output(baos)) {
            typeMetadata.write(kryo, output);
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    /**
     * Deserializes a {@link TypeMetadata} previously written by {@link #serialize(TypeMetadata)}.
     *
     * @param serialized
     *            a Base64-encoded Kryo byte stream produced by {@link #serialize(TypeMetadata)}
     * @return the deserialized {@link TypeMetadata}
     */
    public TypeMetadata deserialize(String serialized) {
        TypeMetadata typeMetadata = new TypeMetadata();
        byte[] bytes = Base64.getDecoder().decode(serialized);
        try (Input input = new Input(bytes)) {
            typeMetadata.read(kryo, input);
        }
        return typeMetadata;
    }
}
