package datawave.ingest.mapreduce.job;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory for creating and managing a singleton SplitsCache instance.
 *
 * Provides lazy initialization with thread-safe access to minimize memory footprint.
 * All code in the JVM shares the same cache instance to avoid duplicating large splits file in memory.
 *
 * Custom implementations can be provided via configuration property {@value #SPLITS_CACHE_IMPL}.
 * Custom implementations must:
 * - Implement the SplitsCache interface
 * - Have a public no-argument constructor
 * - Be available on the classpath
 *
 * Note: Tests should call {@link #clearInstance()} in cleanup to avoid cross-test pollution.
 */
public class SplitsCacheFactory {
    public static final String SPLITS_CACHE_IMPL = "datawave.ingest.splits.cache.impl";

    static volatile SplitsCache INSTANCE;

    /**
     * Get or create the singleton SplitsCache using double-checked locking.
     *
     * On first call, instantiates the implementation class (from config or SplitsFile default),
     * initializes it with the configuration, and caches the instance for reuse.
     *
     * @param conf the configuration containing optional {@value #SPLITS_CACHE_IMPL} property
     * @return the singleton SplitsCache instance
     * @throws RuntimeException if the configured implementation cannot be found or instantiated
     */
    public static SplitsCache getSplitsCache(final Configuration conf) {
        if (INSTANCE == null) {
            synchronized (SplitsCacheFactory.class) {
                if (INSTANCE == null) {
                    try {
                        String splitsCacheImpl = conf.get(SPLITS_CACHE_IMPL);
                        // noinspection unchecked
                        Class<? extends SplitsCache> clazz = splitsCacheImpl != null ? (Class<? extends SplitsCache>) Class.forName(splitsCacheImpl)
                                        : SplitsFile.class;
                        INSTANCE = clazz.getDeclaredConstructor().newInstance();
                        INSTANCE.init(conf);
                    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Clear the singleton instance, forcing re-initialization on next access.
     *
     * Primarily used by tests to avoid cross-test pollution from singleton state.
     * Should be called in test cleanup methods.
     */
    public static void clearInstance() {
        INSTANCE = null;
    }
}
