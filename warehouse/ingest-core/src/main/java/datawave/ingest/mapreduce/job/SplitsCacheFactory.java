package datawave.ingest.mapreduce.job;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;

public class SplitsCacheFactory {
    public static final String SPLITS_CACHE_IMPL = "datawave.ingest.splits.cache.impl";

    static volatile SplitsCache INSTANCE;

    public static SplitsCache getSplitsCache(final Configuration conf) {
        if (INSTANCE == null) {
            synchronized (SplitsCacheFactory.class) {
                if (INSTANCE == null) {
                    try {
                        String splitsCacheImpl = conf.get(SPLITS_CACHE_IMPL);
                        // noinspection unchecked
                        Class<? extends SplitsCache> clazz = splitsCacheImpl != null ? (Class<? extends SplitsCache>) Class.forName(splitsCacheImpl) : SplitsFile.class;
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

    public static void clearInstance() {
        INSTANCE = null;
    }
}
