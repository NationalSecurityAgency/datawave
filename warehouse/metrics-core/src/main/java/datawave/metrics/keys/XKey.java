package datawave.metrics.keys;

import org.apache.accumulo.core.data.Key;

/**
 * Extensible key interface. Implementations should be able to parse the text and should provide POJO representations of the key object.
 *
 */
public interface XKey {
    void parse(Key k) throws InvalidKeyException;

    Key toKey();
}
