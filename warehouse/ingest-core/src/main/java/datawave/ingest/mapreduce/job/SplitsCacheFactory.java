package datawave.ingest.mapreduce.job;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory for the singleton SplitsCache instance, lazily initialized and shared across the JVM to avoid duplicating the splits file in memory.
 *
 * A custom implementation can be set via {@value #SPLITS_CACHE_IMPL}; it must implement SplitsCache with a public no-argument constructor on the classpath.
 *
 * Tests should call {@link #clearInstance()} in cleanup to avoid cross-test pollution.
 */
public class SplitsCacheFactory {
    public static final String SPLITS_CACHE_IMPL = "datawave.ingest.splits.cache.impl";

    static volatile SplitsCache INSTANCE;

    /**
     * Get or create the singleton SplitsCache using double-checked locking, instantiating the configured implementation (or SplitsFile by default) on first
     * call.
     *
     * @param conf
     *            the configuration containing optional {@value #SPLITS_CACHE_IMPL} property
     * @return the singleton SplitsCache instance
     * @throws RuntimeException
     *             if the configured implementation cannot be found or instantiated
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
     * Clears the singleton instance so the next access reinitializes it. Used by tests to avoid cross-test pollution.
     */
    public static void clearInstance() {
        INSTANCE = null;
    }
}
