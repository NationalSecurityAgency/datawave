package datawave.core.cache;

/**
 * An interface that defines a {@link Class} cache.
 * <p>
 * All caches must support threadsafe operations.
 * <p>
 * No implementation should rely on static variables.
 */
public interface ClassCache {

    /**
     * The default cache size
     */
    int DEFAULT_SIZE = 128;

    /**
     * The name of the cache implementation
     *
     * @return the name
     */
    String name();

    /**
     * Get or create a Class for the provided name
     *
     * @param name
     *            the class name
     * @return a {@link Class}
     */
    Class<?> get(String name) throws ClassNotFoundException;
}
