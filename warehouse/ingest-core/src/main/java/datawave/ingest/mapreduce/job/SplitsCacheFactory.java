package datawave.ingest.mapreduce.job;

import static datawave.ingest.mapreduce.job.SplitsConstants.DEFAULT_SPLITS_CACHE_IMPL;
import static datawave.ingest.mapreduce.job.SplitsConstants.SPLITS_CACHE_IMPL;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;

public class SplitsCacheFactory {
    static volatile SplitsCache INSTANCE;

    public static SplitsCache getSplitsCache(final Configuration conf) {
        if (INSTANCE == null) {
            synchronized (SplitsCacheFactory.class) {
                if (INSTANCE == null) {
                    try {
                        final String splitsCacheImpl = conf.get(SPLITS_CACHE_IMPL, DEFAULT_SPLITS_CACHE_IMPL);
                        // noinspection unchecked
                        final Class<? extends SplitsCache> clazz = (Class<? extends SplitsCache>) Class.forName(splitsCacheImpl);
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
}
