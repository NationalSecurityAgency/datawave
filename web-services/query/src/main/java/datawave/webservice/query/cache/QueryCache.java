package datawave.webservice.query.cache;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import datawave.webservice.query.runner.RunningQuery;

/**
 * A cache for storing {@link RunningQuery} objects.
 */
@Singleton
// CDI singleton
public class QueryCache extends AbstractQueryCache<RunningQuery> {
    private ConcurrentHashMap<String,String> locks;

    @Override
    @PostConstruct
    public void init() {
        super.init();
        locks = new ConcurrentHashMap<>();
    }

    @Override
    protected Cache<String,RunningQuery> buildCache() {
        return CacheBuilder.newBuilder().concurrencyLevel(1000).build();
    }

    /**
     * "Locks" {@code id}. This is really just a marker to indicate whether or not {@code id} is locked or not. That is, if {@code id} is not locked, this
     * method will return {@code true}. If {@code id} is already locked, then this method will return {@code false}.
     *
     * @param id
     *            an id
     * @return a boolean
     */
    public boolean lock(String id) {
        // If the return value from putIfAbsent is null, that means there was no previous entry and
        // therefore the id was not locked, so we can acquire the lock. If there was a previous value
        // (which should always match our id), then the id was previously locked.
        String oldValue = locks.putIfAbsent(id, id);
        return oldValue == null;
    }

    /**
     * Unlocks {@code id}, which is assumed to have been previously locked with a call to {@link #lock(String)}.
     *
     * @param id
     *            an id
     */
    public void unlock(String id) {
        locks.remove(id);
    }
}
