package datawave.core.cache;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * An interface that defines a {@link ColumnVisibility} cache.
 * <p>
 * All caches must support threadsafe operations.
 * <p>
 * No implementation should rely on static variables.
 */
public interface CVCache {

    /**
     * The default cache size
     */
    int DEFAULT_SIZE = 256;

    /**
     * The name of the cache implementation
     *
     * @return the name
     */
    String name();

    /**
     * Get or create a ColumnVisibility for the provided ByteSequence
     *
     * @param bytes
     *            a {@link ByteSequence}
     * @return a {@link ColumnVisibility}
     */
    ColumnVisibility get(ByteSequence bytes);
}
