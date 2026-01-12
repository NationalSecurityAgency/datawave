package datawave.core.iterators.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.data.Key;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Base class that contains helper methods for serializing and deserializing event keys.
 * <p>
 * Handles reading and writing the version and any header information
 */
public abstract class BaseKeySerializer implements VersionedKeySerializer {

    // write versions as a single byte
    public static final byte VERSION_ONE = 0x01;
    public static final byte VERSION_TWO = 0x02;
    public static final byte VERSION_THREE = 0x03;

    // optimization for serializing the full field once, and then an offset for all future instances
    protected final byte FIELD_VALUE = 0x00;
    protected final byte FIELD_OFFSET = 0x01;
    protected final byte FIELD_NOTATION_VALUE = 0x02;
    protected final byte FIELD_NOTATION_OFFSET = 0x03;

    /**
     * Write the {@link #version()} to the output stream
     *
     * @param output
     *            the {@link Output}
     */
    @Override
    public void writeVersion(Output output) {
        output.writeByte(version());
    }

    /**
     * Read the {@link #version()} from the input stream
     *
     * @param input
     *            the {@link Input}
     * @return the version
     */
    @Override
    public byte readVersion(Input input) {
        return input.readByte();
    }

    @Override
    public byte[] write(List<Key> keys) {
        try (var baos = new ByteArrayOutputStream(4096)) {
            try (var output = new Output(baos)) {
                writeVersion(output);
                writePayload(output, keys);
                output.flush();
            }
            baos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Key> read(Key workKey, byte[] data) {
        try (Input input = new Input(data)) {
            readVersion(input);
            return readPayload(input, workKey);
        }
    }
}
