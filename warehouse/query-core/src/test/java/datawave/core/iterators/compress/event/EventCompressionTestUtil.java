package datawave.core.iterators.compress.event;

import static datawave.core.iterators.compress.CompressionTestUtil.iterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.commons.lang3.tuple.Pair;

import datawave.core.iterators.compress.KeyGroup;
import datawave.test.MacTestUtil;

/**
 * Collection of common utility methods used to test the {@link EventSerializationIterator}.
 */
public class EventCompressionTestUtil {

    /**
     * Add the {@link EventSerializationIterator} to the provided table with a default version of 1.
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     */
    public static void addSerializationIterator(TableOperations tops, String tableName) {
        Map<String,String> properties = new HashMap<>();
        for (IteratorScope scope : IteratorScope.values()) {
            String iterator = "table.iterator." + scope.name() + ".serialize";
            String value = "18,datawave.core.iterators.compress.event.EventSerializationIterator";
            properties.put(iterator, value);

            String version = "table.iterator." + scope.name() + ".serialize.opt.version";
            properties.put(version, "1");

            String compressionThreshold = "table.iterator." + scope.name() + ".serialize.opt.threshold";
            properties.put(compressionThreshold, "512");

            String compressionAlgorithm = "table.iterator." + scope.name() + ".serialize.opt.algorithm";
            properties.put(compressionAlgorithm, "gzip");
        }
        MacTestUtil.addPropertiesAndWait(tops, tableName, properties);
    }

    /**
     * Remove the {@link EventSerializationIterator} from the provided table. Also removes the version option.
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     */
    public static void removeSerializationIterator(TableOperations tops, String tableName) {
        Set<String> properties = new HashSet<>();
        for (IteratorScope scope : IteratorScope.values()) {
            String iterator = "table.iterator." + scope.name() + ".serialize";
            properties.add(iterator);

            String versionProperty = "table.iterator." + scope.name() + ".serialize.opt.version";
            properties.add(versionProperty);
        }
        MacTestUtil.removePropertiesAndWait(tops, tableName, properties);
    }

    /**
     * Set the {@link EventSerializationIterator#VERSION_OPT} for the serialization iterator
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     * @param version
     *            the serialization version
     */
    public static void setSerializationVersion(TableOperations tops, String tableName, int version) {
        Map<String,String> properties = new HashMap<>();
        for (IteratorScope scope : IteratorScope.values()) {
            String versionProperty = "table.iterator." + scope.name() + ".serialize.opt.version";
            properties.put(versionProperty, String.valueOf(version));
        }
        MacTestUtil.addPropertiesAndWait(tops, tableName, properties);
    }

    /**
     * Set the {@link EventSerializationIterator#THRESHOLD_OPT} for the serialization iterator
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     * @param threshold
     *            the threshold in bytes after which an event is compressed
     */
    public static void setCompressionThreshold(TableOperations tops, String tableName, int threshold) {
        Map<String,String> properties = new HashMap<>();
        for (IteratorScope scope : IteratorScope.values()) {
            String compressionThreshold = "table.iterator." + scope.name() + ".serialize.opt.threshold";
            properties.put(compressionThreshold, String.valueOf(threshold));
        }
        MacTestUtil.addPropertiesAndWait(tops, tableName, properties);
    }

    /**
     * Set the {@link EventSerializationIterator#ALGORITHM_OPT} for the serialization iterator
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     * @param algorithm
     *            the compression algorithm
     */
    public static void setCompressionAlgorithm(TableOperations tops, String tableName, String algorithm) {
        Map<String,String> properties = new HashMap<>();
        for (IteratorScope scope : IteratorScope.values()) {
            String compressionAlgorithm = "table.iterator." + scope.name() + ".serialize.opt.algorithm";
            properties.put(compressionAlgorithm, algorithm);
        }
        MacTestUtil.addPropertiesAndWait(tops, tableName, properties);
    }

    /**
     * Remove the {@link EventSerializationIterator#THRESHOLD_OPT} for the serialization iterator
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     */
    public static void removeCompressionThreshold(TableOperations tops, String tableName) {
        Set<String> properties = new HashSet<>();
        for (IteratorScope scope : IteratorScope.values()) {
            String compressionThreshold = "table.iterator." + scope.name() + ".serialize.opt.threshold";
            properties.add(compressionThreshold);
        }
        MacTestUtil.removePropertiesAndWait(tops, tableName, properties);
    }

    /**
     * Add the {@link EventDeserializationIterator} to the provided table
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     */
    public static void addDeserializationIterator(TableOperations tops, String tableName) {
        // only scan gets the decompression iterator. minc and majc should produce compressed keys
        IteratorScope scope = IteratorScope.scan;
        String iterator = "table.iterator." + scope.name() + ".deserialize";
        String value = "19,datawave.core.iterators.compress.event.EventDeserializationIterator";
        MacTestUtil.addPropertiesAndWait(tops, tableName, Collections.singletonMap(iterator, value));
    }

    /**
     * Remove the {@link EventDeserializationIterator} from the provided table.
     *
     * @param tops
     *            the {@link TableOperations}
     * @param tableName
     *            the table name
     */
    public static void removeDeserializationIterator(TableOperations tops, String tableName) {
        IteratorScope scope = IteratorScope.scan;
        String iterator = "table.iterator." + scope.name() + ".deserialize";
        MacTestUtil.removePropertiesAndWait(tops, tableName, Set.of(iterator));
    }

    /**
     * Compress a Key using the {@link EventSerializationUtil} and a default version of one.
     *
     * @param key
     *            the key
     * @return a key value pair
     * @throws IOException
     *             if something goes wrong
     */
    public static Pair<Key,Value> serialize(Key key) throws IOException {
        List<Pair<Key,Value>> data = serialize(key, 1);
        assertEquals(1, data.size(), "expected one key but there were " + data.size());
        return data.get(0);
    }

    /**
     * Compress a Key using the {@link EventSerializationUtil} and the provided version
     *
     * @param key
     *            the key
     * @param version
     *            the serialization version
     * @return a list of key value pairs
     */
    public static List<Pair<Key,Value>> serialize(Key key, int version) {
        try {
            EventSerializationUtil util = new EventSerializationUtil();
            util.setSerializationVersion(version);

            KeyGroup serialized = util.serialize(iterator(List.of(key)));
            return serialized.getKeyValues();
        } catch (Exception e) {
            fail("Failed to compress keys", e);
            throw new RuntimeException(e);
        }
    }

    public static List<Pair<Key,Value>> serialize(List<Key> keys) {
        try {
            EventSerializationUtil util = new EventSerializationUtil();
            util.setSerializationVersion(1);

            KeyGroup compressed = util.serialize(iterator(keys));
            return compressed.getKeyValues();
        } catch (Exception e) {
            fail("Failed to compress keys", e);
            throw new RuntimeException(e);
        }
    }

}
