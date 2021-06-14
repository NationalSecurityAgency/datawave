package datawave.microservice.query.status;

import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryStorageLock;
import datawave.microservice.query.config.QueryProperties;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;

import java.util.UUID;

import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.CREATED;

public class QueryStatusUpdateHelper {
    
    private final QueryProperties queryProperties;
    private final QueryStorageCache queryStorageCache;
    
    public QueryStatusUpdateHelper(QueryProperties queryProperties, QueryStorageCache queryStorageCache) {
        this.queryProperties = queryProperties;
        this.queryStorageCache = queryStorageCache;
    }
    
    public void claimConcurrentNext(QueryStatus queryStatus) throws QueryException {
        // we can only call next on a created query
        if (queryStatus.getQueryState() == CREATED) {
            // increment the concurrent next count
            if (queryStatus.getConcurrentNextCount() < queryProperties.getNextCall().getConcurrency()) {
                queryStatus.setConcurrentNextCount(queryStatus.getConcurrentNextCount() + 1);
                
                // update the last used datetime for the query
                queryStatus.setLastUsedMillis(System.currentTimeMillis());
            } else {
                throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR,
                                "Concurrent next call limit reached: " + queryProperties.getNextCall().getConcurrency());
            }
        } else {
            throw new QueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, "Unable to find query status in cache with CREATED state.");
        }
    }
    
    public void releaseConcurrentNext(QueryStatus queryStatus) throws QueryException {
        // decrement the concurrent next count
        if (queryStatus.getConcurrentNextCount() > 0) {
            queryStatus.setConcurrentNextCount(queryStatus.getConcurrentNextCount() - 1);
            
            // update the last used datetime for the query
            queryStatus.setLastUsedMillis(System.currentTimeMillis());
        } else {
            throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR,
                            "Concurrent next count can't be decremented: " + queryStatus.getConcurrentNextCount());
        }
    }
    
    public QueryStatus lockedUpdate(String queryUUID, StatusUpdater updater) throws QueryException, InterruptedException {
        QueryStatus queryStatus = null;
        QueryStorageLock statusLock = queryStorageCache.getQueryStatusLock(queryUUID);
        if (statusLock.tryLock(queryProperties.getLockWaitTimeMillis(), queryProperties.getLockLeaseTimeMillis())) {
            try {
                queryStatus = queryStorageCache.getQueryStatus(queryUUID);
                if (queryStatus != null) {
                    updater.apply(queryStatus);
                    queryStorageCache.updateQueryStatus(queryStatus);
                } else {
                    throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, "Unable to find query status in cache.");
                }
            } finally {
                statusLock.unlock();
            }
        } else {
            updater.onLockFailed();
        }
        return queryStatus;
    }
}
