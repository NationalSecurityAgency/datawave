package datawave.microservice.query.cachedresults.status.cache.util;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

public interface CacheUpdater<T> {
    void apply(T cacheEntry) throws QueryException;
    
    default void onLockFailed() throws QueryException {
        throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR, "Unable to acquire lock on cache entry");
    }
}
