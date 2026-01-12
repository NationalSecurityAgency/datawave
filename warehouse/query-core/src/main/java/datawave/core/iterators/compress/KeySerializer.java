package datawave.core.iterators.compress;

import java.util.List;

import org.apache.accumulo.core.data.Key;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Wrapper around Kryo's {@link Input} and {@link Output}.
 */
public interface KeySerializer {

    /**
     * Write the list of keys to a byte array. It is assumed that all keys are grouped correctly, i.e. same row, column family and visibility
     *
     * @param keys
     *            the list of keys to serialize
     * @return a byte array
     */
    byte[] write(List<Key> keys);

    /**
     * Read a list of keys from a data buffer
     *
     * @param workKey
     *            the work key
     * @param data
     *            the byte array
     * @return a list of keys
     */
    List<Key> read(Key workKey, byte[] data);
}
