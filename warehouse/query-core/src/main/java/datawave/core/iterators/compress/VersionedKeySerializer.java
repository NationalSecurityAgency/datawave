package datawave.core.iterators.compress;

import java.util.List;

import org.apache.accumulo.core.data.Key;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A {@link KeySerializer} that supports versioning.
 * <p>
 * This allows the {@link MetaKeySerializer} to dynamically select the appropriate serializer.
 * <p>
 * <b>NOTE:</b> while an interface cannot be marked final in Java, please be extremely careful when making changes to this interface. Data that is serialized
 * prior to any changes may not be deserializable, and you risk losing that data forever.
 */
public interface VersionedKeySerializer extends KeySerializer {

    /**
     * Get the version
     *
     * @return the version
     */
    byte version();

    /**
     * Write the {@link #version()} to the output stream
     *
     * @param output
     *            the Kryo {@link Output}
     */
    void writeVersion(Output output);

    /**
     * Read the {@link #version()} from the input stream
     *
     * @param input
     *            the Kryo {@link Input}
     * @return the version
     */
    byte readVersion(Input input);

    /**
     * Write the payload
     *
     * @param output
     *            the Kryo {@link Output}
     * @param keys
     *            the list of keys to serialize
     */
    void writePayload(Output output, List<Key> keys);

    /**
     * Read the payload from the provided Kryo {@link Input} stream
     *
     * @param input
     *            the Kryo {@link Input}
     * @param workKey
     *            the work key
     * @return the list of deserialized keys
     */
    List<Key> readPayload(Input input, Key workKey);
}
