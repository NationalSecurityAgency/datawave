package datawave.core.iterators.compress.event;

import static datawave.core.iterators.compress.event.EventMarkerUtil.GZIP_FAILED;
import static datawave.core.iterators.compress.event.EventMarkerUtil.RAW_MARKER;
import static datawave.core.iterators.compress.event.EventMarkerUtil.ZSTD_FAILED;
import static datawave.core.iterators.compress.event.EventMarkerUtil.getCompressionAlgorithm;
import static datawave.core.iterators.compress.event.EventMarkerUtil.isMarker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.core.iterators.compress.CompressionUtil;
import datawave.core.iterators.compress.KeyGroup;
import datawave.core.iterators.compress.KeySerializer;
import datawave.core.iterators.compress.MetaKeySerializer;

/**
 * Utility class used to serialize and deserialize a {@link KeyGroup}.
 * <p>
 * <b>NOTE:</b> this class is not thread safe.
 */
@NotThreadSafe
public class EventSerializationUtil {

    private static final Logger log = LoggerFactory.getLogger(EventSerializationUtil.class);

    private static final Value EMPTY_VALUE = new Value();

    private final MetaKeySerializer serializer = new MetaKeySerializer();
    private final KeySerializer deserializer = new MetaKeySerializer();

    private String versionString;

    private int compressionThreshold = Integer.MAX_VALUE;
    private String compressionAlgorithm = RAW;

    public static final String RAW = "raw";
    public static final String GZIP = "gzip";
    public static final String ZSTD = "zstd";
    private final Set<String> supportedCompressionAlgorithms = Set.of(RAW, GZIP, ZSTD);

    public EventSerializationUtil() {
        setSerializationVersion(1);
    }

    /**
     * Set the serialization version. See {@link MetaKeySerializer} for more details.
     *
     * @param version
     *            the version
     */
    public void setSerializationVersion(int version) {
        this.serializer.setIndex(version);
        this.versionString = String.valueOf(version);
    }

    /**
     * Set the compression threshold in bytes. Events that exceed the threshold are compressed according to the {@link #compressionAlgorithm}
     *
     * @param compressionThreshold
     *            the compression threshold in bytes
     */
    public void setCompressionThreshold(int compressionThreshold) {
        if (compressionThreshold <= 0) {
            throw new IllegalArgumentException("compressionThreshold must be >= 0");
        }
        this.compressionThreshold = compressionThreshold;
    }

    /**
     * Set the compression algorithm. See {@link #supportedCompressionAlgorithms} for which algorithms are allowed.
     *
     * @param compressionAlgorithm
     *            the compression algorithm
     */
    public void setCompressionAlgorithm(String compressionAlgorithm) {
        if (supportedCompressionAlgorithms.contains(compressionAlgorithm)) {
            this.compressionAlgorithm = compressionAlgorithm;
        } else {
            throw new IllegalArgumentException("compressionAlgorithm must be one of: " + supportedCompressionAlgorithms);
        }
    }

    /**
     * Validate options
     */
    public void validateOptions() {
        if (compressionThreshold <= 0) {
            throw new IllegalArgumentException("compressionThreshold must be >= 0");
        }
        if (compressionAlgorithm == null) {
            throw new IllegalArgumentException("compressionAlgorithm must be set");
        }
    }

    /**
     * Determine if the provided key is an event key
     *
     * @param key
     *            the key
     * @return true if the key is an event key
     */
    public static boolean isEventKey(Key key) {
        if (key == null) {
            return false;
        }
        String cf = key.getColumnFamily().toString();
        return !(cf.isBlank() || cf.startsWith("fi\0") || cf.equals("tf") || cf.equals("d"));
    }

    /**
     * Aggregates the current {@link PartialKey#ROW_COLFAM} and serializes the event keys into a {@link KeyGroup}, optionally compressing the serialized byte
     * array.
     * <p>
     * <b>WARNING:</b> the caller should ensure that this method is only called on event keys
     *
     * @param iterator
     *            a {@link SortedKeyValueIterator}
     * @return a {@link KeyGroup}
     * @throws IOException
     *             if something goes wrong
     */
    public KeyGroup serialize(SortedKeyValueIterator<Key,Value> iterator) throws IOException {
        if (!iterator.hasTop()) {
            throw new RuntimeException("iterator has no top element");
        }

        Key workKey = iterator.getTopKey();
        Multimap<Key,Pair<Key,Value>> aggregate = TreeMultimap.create(new EventKeyComparator(), Comparator.naturalOrder());

        while (iterator.hasTop() && workKey.equals(iterator.getTopKey(), PartialKey.ROW_COLFAM)) {
            Key tk = iterator.getTopKey();
            Value tv = new Value(iterator.getTopValue().get());
            aggregate.put(tk, Pair.of(tk, tv));
            iterator.next();
        }

        KeyGroup compressed = new KeyGroup();
        for (Key key : new TreeSet<>(aggregate.keySet())) {
            Collection<Pair<Key,Value>> pairs = aggregate.get(key);

            if (!isDecompressionRequired(pairs)) {
                for (Pair<Key,Value> pair : pairs) {
                    compressed.addKeyValue(pair.getLeft(), pair.getRight());
                }
                continue;
            }

            List<Key> data = new ArrayList<>();
            for (Pair<Key,Value> pair : pairs) {
                String cq = pair.getKey().getColumnQualifier().toString();
                if (isMarker(cq)) {
                    byte[] bytes = pair.getValue().get();
                    String algorithm = getCompressionAlgorithm(cq);
                    switch (algorithm) {
                        case GZIP:
                            bytes = CompressionUtil.decompressGZIP(bytes);
                            break;
                        case ZSTD:
                            bytes = CompressionUtil.decompressZSTD(bytes);
                            break;
                        case RAW:
                        case GZIP_FAILED:
                        case ZSTD_FAILED:
                            // do nothing
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown compression algorithm: " + algorithm);
                    }

                    List<Key> deserialized = deserializer.read(key, bytes);
                    data.addAll(deserialized);
                } else {
                    data.add(pair.getKey());
                }
            }

            Pair<String,byte[]> serializedData = serializeKeys(data);
            Key compressedMarker = EventMarkerUtil.createMarker(key, serializedData.getLeft(), versionString, data.size());
            compressed.addKeyValue(compressedMarker, new Value(serializedData.getRight()));
        }

        return compressed;
    }

    /**
     * Decompression is required when:
     * <ul>
     * <li>the compression algorithm changes</li>
     * <li>the serialization version changes</li>
     * <li>multiple pairs exist</li>
     * </ul>
     * changing the serialization version, the compression threshold, the compression algorithm or when merging multiple keys.
     *
     * @param pairs
     *            the list of key value pairs
     * @return true if decompression is required
     */
    protected boolean isDecompressionRequired(Collection<Pair<Key,Value>> pairs) {

        if (pairs.size() > 1) {
            return true;
        }

        for (Pair<Key,Value> pair : pairs) {
            String cq = pair.getKey().getColumnQualifier().toString();
            String first = EventMarkerUtil.getCompressionAlgorithm(cq);
            // startsWith covers both the equality case and the failed compression case
            if (!first.startsWith(compressionAlgorithm)) {
                return true;
            }

            String version = EventMarkerUtil.getSerializationVersion(cq);
            if (!versionString.equals(version)) {
                return true;
            }
        }

        // single key matches the compression algorithm, or previous failed to compress small
        return false;
    }

    /**
     * Serialize the list of keys into a byte array. The compression algorithm is returned with the byte array for context.
     * <p>
     * In the event that a compressed byte array is longer than the original a 'failed' marker is used. This prevents the serialization utility from attempting
     * to repeatedly compress the byte array.
     *
     * @param keys
     *            the list of keys
     * @return a pair of compression algorithm and serialized byte array
     */
    protected Pair<String,byte[]> serializeKeys(List<Key> keys) {
        String compression = EventMarkerUtil.RAW_MARKER;
        byte[] serializedData = serializer.write(keys);
        if (serializedData.length > compressionThreshold) {
            switch (compressionAlgorithm) {
                case RAW_MARKER:
                    break; // do nothing
                case GZIP:
                    byte[] gzipCandidate = CompressionUtil.compressGZIP(serializedData);
                    if (gzipCandidate.length < serializedData.length) {
                        compression = EventMarkerUtil.GZIP_MARKER;
                        serializedData = gzipCandidate;
                    } else {
                        compression = EventMarkerUtil.GZIP_FAILED;
                    }
                    break;
                case ZSTD:
                    byte[] zstdCandidate = CompressionUtil.compressZSTD(serializedData);
                    if (zstdCandidate.length < serializedData.length) {
                        compression = EventMarkerUtil.ZSTD_MARKER;
                        serializedData = CompressionUtil.compressZSTD(serializedData);
                    } else {
                        compression = EventMarkerUtil.ZSTD_FAILED;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("compressionAlgorithm must be one of: " + supportedCompressionAlgorithms);
            }
        }
        return Pair.of(compression, serializedData);
    }

    /**
     * Aggregates the current {@link PartialKey#ROW_COLFAM} and deserializes any serialized keys, even if the serialized byte arrays were compressed.
     * <p>
     * <b>WARNING:</b> the caller should ensure that this method is only called on event keys
     *
     * @param iterator
     *            a {@link SortedKeyValueIterator}
     * @return an {@link Iterator} of key values
     * @throws IOException
     *             if something goes wrong
     */
    public Iterator<Map.Entry<Key,Value>> deserialize(SortedKeyValueIterator<Key,Value> iterator) throws IOException {
        Key workKey = iterator.getTopKey();
        Multimap<Key,Value> map = TreeMultimap.create();
        while (iterator.hasTop() && workKey.equals(iterator.getTopKey(), PartialKey.ROW_COLFAM)) {
            Key tk = iterator.getTopKey();
            Value tv = iterator.getTopValue();

            String cq = tk.getColumnQualifier().toString();
            if (EventMarkerUtil.isMarker(cq)) {
                byte[] data = tv.get();
                String marker = EventMarkerUtil.getCompressionAlgorithm(cq);

                switch (marker) {
                    case EventMarkerUtil.RAW_MARKER:
                        // key is not compressed, nothing to do
                        break;
                    case EventMarkerUtil.GZIP_MARKER:
                        data = CompressionUtil.decompressGZIP(data);
                        break;
                    case EventMarkerUtil.ZSTD_MARKER:
                        data = CompressionUtil.decompressZSTD(data);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown compression algorithm: " + marker);
                }

                List<Key> keys = deserializer.read(tk, data);
                keys.forEach(key -> map.put(key, EMPTY_VALUE));
            } else {
                map.put(tk, tv);
            }

            iterator.next();
        }

        return map.entries().iterator();
    }
}
