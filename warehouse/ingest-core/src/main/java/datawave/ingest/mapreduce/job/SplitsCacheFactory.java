package datawave.ingest.mapreduce.job;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Factory for SplitsCache instances, lazily initialized and keyed by resolved splits path. Configurations that resolve to the same splits path share one
 * instance (avoiding duplicating the splits file in memory); configurations that resolve to different paths each get their own, correctly initialized instance
 * instead of one silently winning and the other being ignored.
 *
 * A custom implementation can be set via {@value #SPLITS_CACHE_IMPL}; it must implement SplitsCache with a public no-argument constructor on the classpath.
 *
 * Tests should call {@link #clearInstance()} in cleanup to avoid cross-test pollution.
 */
public class SplitsCacheFactory {
    public static final String SPLITS_CACHE_IMPL = "datawave.ingest.splits.cache.impl";

    private static final ConcurrentHashMap<Path,SplitsCache> INSTANCES = new ConcurrentHashMap<>();

    /**
     * Get or create the SplitsCache for the splits path resolved from conf, instantiating the configured implementation (or SplitsFile by default) the first
     * time that path is requested. {@code computeIfAbsent} guarantees only one thread builds a given path's instance, and that a failed build leaves no entry
     * behind for the next caller to retry.
     *
     * @param conf
     *            the configuration containing optional {@value #SPLITS_CACHE_IMPL} property
     * @return the SplitsCache instance for the resolved splits path
     * @throws RuntimeException
     *             if the configured implementation cannot be found or instantiated
     */
    public static SplitsCache getSplitsCache(final Configuration conf) {
        return INSTANCES.computeIfAbsent(TableSplitsCache.getSplitsPath(conf), path -> buildAndInit(conf));
    }

    private static SplitsCache buildAndInit(Configuration conf) {
        try {
            String splitsCacheImpl = conf.get(SPLITS_CACHE_IMPL);
            // noinspection unchecked
            Class<? extends SplitsCache> clazz = splitsCacheImpl != null ? (Class<? extends SplitsCache>) Class.forName(splitsCacheImpl) : SplitsFile.class;
            SplitsCache candidate = clazz.getDeclaredConstructor().newInstance();
            candidate.init(conf);
            return candidate;
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Clears all cached instances so the next access reinitializes them. Used by tests to avoid cross-test pollution.
     */
    public static void clearInstance() {
        INSTANCES.clear();
    }
}
