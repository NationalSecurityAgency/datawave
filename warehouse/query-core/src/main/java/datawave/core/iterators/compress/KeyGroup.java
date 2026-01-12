package datawave.core.iterators.compress;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iteratorsImpl.system.VisibilityFilter;
import org.apache.commons.lang3.tuple.Pair;

import datawave.core.iterators.compress.event.EventSerializationUtil;

/**
 * A {@link KeyGroup} is defined as a series of {@link Key}-{@link Value} pairs.
 * <p>
 * This class is used by the {@link EventSerializationUtil} to store the intermediate results of a compression or decompression operation.
 * <p>
 * A key group is typically defined by a common prefix (row and column family), and optionally a partial column qualifier. Distinct outputs must be created for
 * visibility, timestamp and delete flags in order to not subvert the system level {@link VisibilityFilter} or break the assumptions of the
 * {@link VersioningIterator}
 * <p>
 * Compressed keys should include some visible indication of compression.
 * <p>
 * Compressed values should include all information necessary for decompression, independent of the key.
 */
public class KeyGroup {

    private final List<Pair<Key,Value>> aggregate = new ArrayList<>();

    /**
     * Adds a key-value pair to the key group
     *
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void addKeyValue(Key key, Value value) {
        aggregate.add(Pair.of(key, value));
    }

    /**
     * Get key group
     *
     * @return the key values
     */
    public List<Pair<Key,Value>> getKeyValues() {
        return aggregate;
    }
}
