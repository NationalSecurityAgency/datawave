package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.services.query.configuration.GenericQueryConfiguration;
import datawave.services.query.logic.QueryKey;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * This class will cache a QueryStatus object for a specified query. The underlying storage will be polled at a specified interval, or on demand if the current
 * object is deemed too old. If it is desired that the storage is polled at a specified interval, then use startTimer after construction. Otherwise the query
 * status will be updated when a getter is called iff the current instance is too old. All setter methods except for numResultsReturned, numResultsGenerated,
 * nextCount, and seekCount will force an update and will update the underlying storage. Updates made for the counters specified will only be updated in the
 * underlying storage when the cache is refreshed per the specified interval.
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
        loadQueryStatus();
    }
    
    /**
     * Update the query status, merging in any pending updates and executing the supplied query status updater
     * 
     * @param updater
     */
    public synchronized void updateQueryStatus(Runnable updater) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            forceCacheUpdateInsideSetter();
            updater.run();
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Are there pending updates we need to merge into the query status?
     */
    public boolean hasPendingUpdates() {
        return super.getNumResultsGenerated() > 0 || super.getNumResultsReturned() > 0 || super.getNextCount() > 0 || super.getSeekCount() > 0;
    }
    
    /**
     * Force the cache to be updated. This will also store any pending numResults increments.
     */
    public synchronized void forceCacheUpdate() {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            loadQueryStatus();
            cache.updateQueryStatus(queryStatus);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Force the cache to be updated with any pending numResult increments.
     */
    public synchronized void forceCacheUpdateIfDirty() {
        if (hasPendingUpdates()) {
            forceCacheUpdate();
        }
    }
    
    /**
     * Force the cache to be updated. It is expected that the caller has locked the query status in the cache.
     */
    private synchronized void forceCacheUpdateInsideSetter() {
        loadQueryStatus();
        queryStatus.setLastUpdatedMillis(queryStatusTimeStamp);
    }
    
    /**
     * Load the query status, updating the pending counts if any
     */
    private synchronized void loadQueryStatus() {
        queryStatus = cache.getQueryStatus(queryId);
        if (hasPendingUpdates()) {
            queryStatus.incrementNumResultsGenerated(super.getNumResultsGenerated());
            queryStatus.incrementNumResultsReturned(super.getNumResultsReturned());
            queryStatus.incrementNextCount(super.getNextCount());
            queryStatus.incrementSeekCount(super.getSeekCount());
            super.setNumResultsGenerated(0);
            super.setNumResultsReturned(0);
            super.setNextCount(0);
            super.setSeekCount(0);
        }
        queryStatusTimeStamp = System.currentTimeMillis();
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
    public boolean isProgressIdle(long currentTimeMillis, long idleTimeoutMillis) {
        return get().isProgressIdle(currentTimeMillis, idleTimeoutMillis);
    }
    
    @Override
    public boolean isUserIdle(long currentTimeMillis, long idleTimeoutMillis) {
        return get().isUserIdle(currentTimeMillis, idleTimeoutMillis);
    }
    
    @Override
    public boolean isInactive(long currentTimeMillis, long evictionTimeoutMillis) {
        return get().isInactive(currentTimeMillis, evictionTimeoutMillis);
    }
    
    @Override
    public boolean isRunning() {
        return get().isRunning();
    }
    
    @Override
    public synchronized void setQueryKey(QueryKey key) {
        updateQueryStatus(() -> queryStatus.setQueryKey(key));
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
        updateQueryStatus(() -> queryStatus.setQueryState(queryState));
    }
    
    @Override
    public CREATE_STAGE getCreateStage() {
        return get().getCreateStage();
    }
    
    @Override
    public synchronized void setCreateStage(CREATE_STAGE createStage) {
        updateQueryStatus(() -> queryStatus.setCreateStage(createStage));
    }
    
    @Override
    public String getPlan() {
        return get().getPlan();
    }
    
    @Override
    public synchronized void setPlan(String plan) {
        updateQueryStatus(() -> queryStatus.setPlan(plan));
    }
    
    @Override
    public Set<BaseQueryMetric.Prediction> getPredictions() {
        return get().getPredictions();
    }
    
    @Override
    public void setPredictions(Set<BaseQueryMetric.Prediction> predictions) {
        updateQueryStatus(() -> queryStatus.setPredictions(predictions));
    }
    
    @Override
    public Query getQuery() {
        return get().getQuery();
    }
    
    @Override
    public synchronized void setQuery(Query query) {
        updateQueryStatus(() -> queryStatus.setQuery(query));
    }
    
    @Override
    public ProxiedUserDetails getCurrentUser() {
        return get().getCurrentUser();
    }
    
    @Override
    public synchronized void setCurrentUser(ProxiedUserDetails currentUser) {
        updateQueryStatus(() -> queryStatus.setCurrentUser(currentUser));
    }
    
    @Override
    public GenericQueryConfiguration getConfig() {
        return get().getConfig();
    }
    
    @Override
    public void setConfig(GenericQueryConfiguration config) {
        updateQueryStatus(() -> queryStatus.setConfig(config));
    }
    
    @Override
    public Set<String> getCalculatedAuths() {
        return get().getCalculatedAuths();
    }
    
    @Override
    public synchronized void setCalculatedAuths(Set<String> calculatedAuths) {
        updateQueryStatus(() -> queryStatus.setCalculatedAuths(calculatedAuths));
    }
    
    @Override
    public Set<Authorizations> getCalculatedAuthorizations() {
        return get().getCalculatedAuthorizations();
    }
    
    @Override
    public synchronized void setCalculatedAuthorizations(Set<Authorizations> calculatedAuthorizations) {
        updateQueryStatus(() -> queryStatus.setCalculatedAuthorizations(calculatedAuthorizations));
    }
    
    @Override
    public String getFailureMessage() {
        return get().getFailureMessage();
    }
    
    @Override
    public synchronized void setFailureMessage(String failureMessage) {
        updateQueryStatus(() -> queryStatus.setFailureMessage(failureMessage));
    }
    
    @Override
    public String getStackTrace() {
        return get().getStackTrace();
    }
    
    @Override
    public synchronized void setStackTrace(String stackTrace) {
        updateQueryStatus(() -> queryStatus.setStackTrace(stackTrace));
    }
    
    @Override
    @JsonIgnore
    public synchronized void setFailure(DatawaveErrorCode errorCode, Exception failure) {
        updateQueryStatus(() -> queryStatus.setFailure(errorCode, failure));
    }
    
    @Override
    public long getNumResultsReturned() {
        return get().getNumResultsReturned();
    }
    
    @Override
    public synchronized void setNumResultsReturned(long numResultsReturned) {
        updateQueryStatus(() -> queryStatus.setNumResultsReturned(numResultsReturned));
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
        updateQueryStatus(() -> queryStatus.setNumResultsGenerated(numResultsGenerated));
    }
    
    @Override
    public void incrementNumResultsReturned(long increment) {
        super.incrementNumResultsReturned(increment);
    }
    
    @Override
    public long getNumResultsConsumed() {
        return get().getNumResultsConsumed();
    }
    
    @Override
    public void setNumResultsConsumed(long numResultsConsumed) {
        updateQueryStatus(() -> queryStatus.setNumResultsConsumed(numResultsConsumed));
    }
    
    @Override
    public void incrementNumResultsConsumed(long increment) {
        updateQueryStatus(() -> queryStatus.incrementNumResultsConsumed(increment));
    }
    
    @Override
    public int getActiveNextCalls() {
        return get().getActiveNextCalls();
    }
    
    @Override
    public synchronized void setActiveNextCalls(int activeNextCalls) {
        updateQueryStatus(() -> queryStatus.setActiveNextCalls(activeNextCalls));
    }
    
    @Override
    public DatawaveErrorCode getErrorCode() {
        return get().getErrorCode();
    }
    
    @Override
    public void setErrorCode(DatawaveErrorCode errorCode) {
        updateQueryStatus(() -> queryStatus.setErrorCode(errorCode));
    }
    
    @Override
    public long getLastPageNumber() {
        return get().getLastPageNumber();
    }
    
    @Override
    public synchronized void setLastPageNumber(long lastPageNumber) {
        updateQueryStatus(() -> queryStatus.setLastPageNumber(lastPageNumber));
    }
    
    @Override
    public long getNextCount() {
        return get().getNextCount() + super.getNextCount();
    }
    
    public void incrementNextCount(long cnt) {
        super.incrementNextCount(cnt);
    }
    
    @Override
    public void setNextCount(long nextCount) {
        super.setNextCount(nextCount);
    }
    
    @Override
    public long getSeekCount() {
        return get().getSeekCount() + super.getSeekCount();
    }
    
    @Override
    public void incrementSeekCount(long cnt) {
        super.incrementSeekCount(cnt);
    }
    
    @Override
    public void setSeekCount(long seekCount) {
        super.setSeekCount(seekCount);
    }
    
    @Override
    public long getLastUsedMillis() {
        return get().getLastUsedMillis();
    }
    
    @Override
    public synchronized void setLastUsedMillis(long lastUsedMillis) {
        updateQueryStatus(() -> queryStatus.setLastUsedMillis(lastUsedMillis));
    }
    
    @Override
    public long getLastUpdatedMillis() {
        return get().getLastUpdatedMillis();
    }
    
    @Override
    public synchronized void setLastUpdatedMillis(long lastUpdatedMillis) {
        updateQueryStatus(() -> queryStatus.setLastUpdatedMillis(lastUpdatedMillis));
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
