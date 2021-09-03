package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import datawave.microservice.query.logic.QueryKey;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * This class will cache a QueryStatus object for a specified query. The underlying storage will be polled at a specified interval, or on demand if the current
 * object is deemed too old. If it is desired that the storage is polled at a specified interval, then use startTimer after construction. Otherwise the query
 * status will be updated when a getter is called iff the current instance is too old. All setter methods (exception incrementNumResultsReturned and
 * incrementNumResultsGenerated) will force an update and will update the underlying storage. Updates made via the incrementNumResultReturned and
 * incrementNumResultsGenerated methods will only be updated in the underlying storage when the cache is refreshed per the specified interval.
 */
public class CachedQueryStatus extends QueryStatus {
    private final QueryStorageCache cache;
    private final String queryId;
    private final long invalidCacheMs;
    
    private QueryStatus queryStatus = null;
    private volatile long queryStatusTimeStamp = -1;
    private Timer timer = null;
    
    public CachedQueryStatus(QueryStorageCache cache, String queryId, long invalidCacheMs) {
        this.cache = cache;
        this.queryId = queryId;
        this.invalidCacheMs = invalidCacheMs;
        forceCacheUpdate();
    }
    
    /**
     * Force the cache to be updated. This will also store any pending numResults increments.
     */
    public synchronized void forceCacheUpdate() {
        QueryStorageLock lock = null;
        if (super.getNumResultsGenerated() > 0 || super.getNumResultsReturned() > 0) {
            lock = cache.getQueryStatusLock(queryId);
            lock.lock();
        }
        try {
            queryStatus = cache.getQueryStatus(queryId);
            if (super.getNumResultsGenerated() > 0 || super.getNumResultsReturned() > 0) {
                queryStatus.incrementNumResultsGenerated(super.getNumResultsGenerated());
                queryStatus.incrementNumResultsReturned(super.getNumResultsReturned());
                cache.updateQueryStatus(queryStatus);
                super.setNumResultsGenerated(0);
                super.setNumResultsReturned(0);
            }
            queryStatusTimeStamp = System.currentTimeMillis();
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }
    
    /**
     * Force the cache to be updated with any pending numResult increments.
     */
    public synchronized void forceCacheUpdateIfDirty() {
        if (super.getNumResultsGenerated() > 0 || super.getNumResultsReturned() > 0) {
            forceCacheUpdate();
        }
    }
    
    /**
     * Force the cache to be updated. It is expected that the caller has locked the query status in the cache.
     */
    private synchronized void forceCacheUpdateInsideSetter() {
        queryStatus = cache.getQueryStatus(queryId);
        if (super.getNumResultsGenerated() > 0 || super.getNumResultsReturned() > 0) {
            queryStatus.incrementNumResultsGenerated(super.getNumResultsGenerated());
            queryStatus.incrementNumResultsReturned(super.getNumResultsReturned());
            super.setNumResultsGenerated(0);
            super.setNumResultsReturned(0);
        }
        queryStatusTimeStamp = System.currentTimeMillis();
        queryStatus.setLastUpdatedMillis(queryStatusTimeStamp);
    }
    
    /**
     * Start a timer to update the query status periodically
     */
    public synchronized void startTimer() {
        if (timer == null) {
            timer = new Timer("QueryStatusCache(" + queryId + ")");
            timer.schedule(new TimerTask() {
                public void run() {
                    forceCacheUpdate();
                }
            }, invalidCacheMs, invalidCacheMs);
        }
    }
    
    /**
     * Stop the timer
     */
    public synchronized void stopTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }
    
    /**
     * Get the query status, updating if old.
     * 
     * @return The query status object
     */
    private QueryStatus get() {
        // if not using a timer, and the current status object is old, then update
        if (timer == null && ((queryStatusTimeStamp + invalidCacheMs) < System.currentTimeMillis())) {
            forceCacheUpdate();
        }
        return queryStatus;
    }
    
    @Override
    public synchronized void setQueryKey(QueryKey key) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setQueryKey(key);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public QueryKey getQueryKey() {
        return get().getQueryKey();
    }
    
    @Override
    public QUERY_STATE getQueryState() {
        return get().getQueryState();
    }
    
    @Override
    public synchronized void setQueryState(QUERY_STATE queryState) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setQueryState(queryState);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public String getPlan() {
        return get().getPlan();
    }
    
    @Override
    public synchronized void setPlan(String plan) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setPlan(plan);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public Query getQuery() {
        return get().getQuery();
    }
    
    @Override
    public synchronized void setQuery(Query query) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setQuery(query);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public Set<String> getCalculatedAuths() {
        return get().getCalculatedAuths();
    }
    
    @Override
    public synchronized void setCalculatedAuths(Set<String> calculatedAuths) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setCalculatedAuths(calculatedAuths);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public Set<Authorizations> getCalculatedAuthorizations() {
        return get().getCalculatedAuthorizations();
    }
    
    @Override
    public synchronized void setCalculatedAuthorizations(Set<Authorizations> calculatedAuthorizations) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setCalculatedAuthorizations(calculatedAuthorizations);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public String getFailureMessage() {
        return get().getFailureMessage();
    }
    
    @Override
    public synchronized void setFailureMessage(String failureMessage) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setFailureMessage(failureMessage);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public String getStackTrace() {
        return get().getStackTrace();
    }
    
    @Override
    public synchronized void setStackTrace(String stackTrace) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setStackTrace(stackTrace);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    @JsonIgnore
    public synchronized void setFailure(Exception failure) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setFailure(failure);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public long getNumResultsReturned() {
        return get().getNumResultsReturned();
    }
    
    @Override
    public synchronized void setNumResultsReturned(long numResultsReturned) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setNumResultsReturned(numResultsReturned);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public void incrementNumResultsGenerated(long increment) {
        super.incrementNumResultsGenerated(increment);
    }
    
    @Override
    public long getNumResultsGenerated() {
        return get().getNumResultsGenerated() + super.getNumResultsGenerated();
    }
    
    @Override
    public synchronized void setNumResultsGenerated(long numResultsGenerated) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setNumResultsGenerated(numResultsGenerated);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public void incrementNumResultsReturned(long increment) {
        super.incrementNumResultsReturned(increment);
    }
    
    @Override
    public int getActiveNextCalls() {
        return get().getActiveNextCalls();
    }
    
    @Override
    public synchronized void setActiveNextCalls(int activeNextCalls) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setActiveNextCalls(activeNextCalls);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public long getLastPageNumber() {
        return get().getLastPageNumber();
    }
    
    @Override
    public synchronized void setLastPageNumber(long lastPageNumber) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setLastPageNumber(lastPageNumber);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public long getLastUsedMillis() {
        return get().getLastUsedMillis();
    }
    
    @Override
    public synchronized void setLastUsedMillis(long lastUsedMillis) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setLastUsedMillis(lastUsedMillis);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public long getLastUpdatedMillis() {
        return get().getLastUpdatedMillis();
    }
    
    @Override
    public synchronized void setLastUpdatedMillis(long lastUpdatedMillis) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            queryStatus.setLastUpdatedMillis(lastUpdatedMillis);
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public int hashCode() {
        return get().hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        return get().equals(obj);
    }
    
    @Override
    public String toString() {
        return get().toString();
    }
}
