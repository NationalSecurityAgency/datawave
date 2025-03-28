package datawave.microservice.query.executor.status.cache.util;

import datawave.webservice.query.exception.QueryException;

public interface CacheUpdater<T> {
    void apply(T cacheEntry) throws QueryException;
    
    default void onLockFailed() throws Exception {
        throw new Exception("Unable to acquire lock on cache entry");
    }
}
