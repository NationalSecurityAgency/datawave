package datawave.core.iterators.compress.event;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of utility methods for interacting with markers that denote a serialized and/or compressed event.
 * <p>
 * A marker's structure is the compression algorithm followed by a null byte, then the serialization version, a dash, and then the number of keys serialized in
 * the value.
 * <p>
 * <code>algorithm0x00version-numKeys</code>
 */
public class EventMarkerUtil {

    private static final Logger log = LoggerFactory.getLogger(EventMarkerUtil.class);

    public static final String RAW_MARKER = "raw";
    public static final String GZIP_MARKER = "gzip";
    public static final String ZSTD_MARKER = "zstd";

    // Denote when a compression algorithm fails to produce a shorter byte array. This prevents future compression attempts.
    public static final String GZIP_FAILED = "gzipf";
    public static final String ZSTD_FAILED = "zstdf";

    public static final Set<String> markers = Set.of(RAW_MARKER, GZIP_MARKER, GZIP_FAILED, ZSTD_MARKER, ZSTD_FAILED);

    public static final String DEFAULT_SERIALIZATION_VERSION = "1";

    private EventMarkerUtil() {
        // enforce static access
    }

    /**
     * Determine if the provided key is an event marker
     *
     * @param key
     *            the key
     * @return true if the key is an event marker
     */
    public static boolean isMarker(Key key) {
        String cq = key.getColumnQualifier().toString();
        return isMarker(cq);
    }

    /**
     * Determine if the provided column qualifier is an event marker
     *
     * @param cq
     *            the column qualifier
     * @return true if the provided column qualifier is an event marker
     */
    public static boolean isMarker(Text cq) {
        return isMarker(cq.toString());
    }

    /**
     * Determine if the provided string is an event marker
     *
     * @param cq
     *            the string
     * @return true if the provided string is an event marker
     */
    public static boolean isMarker(String cq) {
        for (String marker : markers) {
            if (cq.startsWith(marker)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the compression algorithm from the provided marker
     *
     * @param cq
     *            the column qualifier
     * @return the compression algorithm
     */
    public static String getCompressionAlgorithm(String cq) {
        int nullIndex = cq.indexOf('\u0000');
        if (nullIndex <= 0) {
            log.debug("malformed event column qualifier: {}", cq);
            return RAW_MARKER;
        }
        return cq.substring(0, nullIndex);
    }

    /**
     * Get the serialization version from the marker
     *
     * @param cq
     *            the column qualifier
     * @return the serialization version
     */
    public static String getSerializationVersion(String cq) {
        int nullIndex = cq.indexOf('\u0000');
        if (nullIndex <= 0) {
            log.debug("event marker missing null index: {}", cq);
            return DEFAULT_SERIALIZATION_VERSION;
        }

        int dashIndex = cq.indexOf('-', nullIndex + 1);
        if (dashIndex <= 0) {
            log.debug("event marker missing dash index: {}", cq);
            return DEFAULT_SERIALIZATION_VERSION;
        }

        String version = cq.substring(nullIndex + 1, dashIndex);
        if (log.isTraceEnabled()) {
            log.trace("serialization version: {}", version);
        }
        return version;
    }

    /**
     * Create a marker key from a work key, serialization version and size. No compression is specified so the {@link #RAW_MARKER} is used.
     *
     * @param key
     *            the work key
     * @param version
     *            the serialization version
     * @param size
     *            the number of keys serialized to the Accumulo value
     * @return an event marker
     */
    public static Key createMarker(Key key, String version, int size) {
        return createMarker(key, RAW_MARKER, version, size);
    }

    /**
     * Create a marker key from a work key, compression algorithm, serialization version and size.
     *
     * @param key
     *            the work key
     * @param compression
     *            the compression algorithm
     * @param version
     *            the serialization version
     * @param size
     *            the number of keys serialized to the Accumulo value
     * @return an event marker
     */
    public static Key createMarker(Key key, String compression, String version, int size) {
        byte[] row = key.getRowData().toArray();
        byte[] cf = key.getColumnFamilyData().toArray();
        byte[] cv = key.getColumnVisibilityData().toArray();
        long ts = key.getTimestamp();

        if (!markers.contains(compression)) {
            throw new IllegalArgumentException("Unknown compression: " + compression);
        }

        Text cq = new Text(compression + '\u0000' + version + '-' + size);
        return new Key(row, cf, cq.copyBytes(), cv, ts, key.isDeleted(), false);
    }

}
